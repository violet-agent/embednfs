use embednfs_proto::{NfsStat4, SequenceArgs4, SequenceRes4};

use super::StateManager;
use super::model::{
    CachedReplay, ClientLeaseState, SequenceCacheToken, SequenceReplay, SessionState, StateInner,
};

/// Outcome of finalizing a slot without awaiting the state lock.
pub(crate) enum TryFinishSequence {
    /// The replay cache entry was stored.
    Finished,
    /// The state lock was held elsewhere; nothing was written and the caller
    /// owns the token and response body again.
    Contended(SequenceCacheToken, Vec<u8>),
    /// The lock was taken but the slot could not be finalized.
    Failed(NfsStat4),
}

impl StateManager {
    /// Takes the state write lock and hands the guard to the caller.
    ///
    /// Only cancellation tests need this: parking a task on the state lock at a
    /// chosen moment is the one way to interrupt `finish_sequence` exactly
    /// where a cancelled worker would be. The guard is opaque so the lock's
    /// interior stays private.
    #[cfg(test)]
    pub(crate) async fn lock_state_for_test(&self) -> impl Send {
        self.inner.write().await
    }

    fn sequence_res(
        session: &SessionState,
        args: &SequenceArgs4,
        status_flags: u32,
    ) -> SequenceRes4 {
        let highest_slot = (session.slots.len() - 1) as u32;
        SequenceRes4 {
            sessionid: args.sessionid,
            sequenceid: args.sequenceid,
            slotid: args.slotid,
            highest_slotid: highest_slot,
            target_highest_slotid: highest_slot,
            status_flags,
        }
    }

    /// Prepare forechannel SEQUENCE handling and classify the request as
    /// a new execution, a retry that should replay a cached reply, or an error.
    #[expect(
        clippy::indexing_slicing,
        reason = "BadSlot is returned locally before indexing the session slot table"
    )]
    pub(crate) async fn prepare_sequence(
        &self,
        args: &SequenceArgs4,
        fingerprint: &[u8],
        connection_id: u64,
    ) -> SequenceReplay {
        let mut inner = self.inner.write().await;
        let now = self.config.now();
        self.reap_expired_clients_locked(&mut inner, now);

        let (clientid, slot_count) = match inner.sessions.get(&args.sessionid) {
            Some(session) => (session.clientid, session.slots.len()),
            None => return SequenceReplay::Error(NfsStat4::BadSession),
        };

        let slot_idx = args.slotid as usize;
        if slot_idx >= slot_count {
            return SequenceReplay::Error(NfsStat4::BadSlot);
        }
        let Some(client) = inner.clients.get(&clientid) else {
            return SequenceReplay::Error(NfsStat4::BadSession);
        };
        if let ClientLeaseState::Revoked { status_flags, .. } = client.lease_state {
            let Some(session) = inner.sessions.get_mut(&args.sessionid) else {
                return SequenceReplay::Error(NfsStat4::BadSession);
            };
            let _ = session.connections.insert(connection_id);
            return SequenceReplay::StatusOnly(Self::sequence_res(session, args, status_flags));
        }

        let replay = {
            let Some(session) = inner.sessions.get_mut(&args.sessionid) else {
                return SequenceReplay::Error(NfsStat4::BadSession);
            };
            let _ = session.connections.insert(connection_id);
            let slot = &mut session.slots[slot_idx];
            let retry_seq = slot.sequence_id.wrapping_sub(1);

            if args.sequenceid == slot.sequence_id {
                if slot.in_progress.is_some() {
                    // RFC 8881 §2.10.6.1: a slot carries at most one
                    // outstanding request. A client that advances the sequence
                    // id while the previous request on the same slot is still
                    // executing is violating that rule; answering NFS4ERR_DELAY
                    // keeps the slot single-threaded instead of running two
                    // requests concurrently against the same replay entry.
                    SequenceReplay::Error(NfsStat4::Delay)
                } else {
                    slot.sequence_id = slot.sequence_id.wrapping_add(1);
                    slot.in_progress = Some(fingerprint.to_vec());
                    slot.cached_reply = None;
                    let res = Self::sequence_res(session, args, 0);
                    SequenceReplay::Execute(
                        res,
                        SequenceCacheToken {
                            sessionid: args.sessionid,
                            slotid: args.slotid,
                            fingerprint: fingerprint.to_vec(),
                        },
                    )
                }
            } else if args.sequenceid != retry_seq {
                SequenceReplay::Error(NfsStat4::SeqMisordered)
            } else if let Some(in_progress) = &slot.in_progress {
                if in_progress == fingerprint {
                    SequenceReplay::Error(NfsStat4::Delay)
                } else {
                    SequenceReplay::Error(NfsStat4::SeqFalseRetry)
                }
            } else if let Some(cached) = &slot.cached_reply {
                if cached.fingerprint == fingerprint {
                    SequenceReplay::Replay(cached.response.clone())
                } else {
                    SequenceReplay::Error(NfsStat4::SeqFalseRetry)
                }
            } else {
                SequenceReplay::Error(NfsStat4::Serverfault)
            }
        };

        if matches!(
            replay,
            SequenceReplay::Execute(_, _) | SequenceReplay::Replay(_)
        ) && let Some(client) = inner.clients.get_mut(&clientid)
        {
            client.lease_state = ClientLeaseState::Active {
                deadline: self.lease_deadline(now),
            };
        }

        replay
    }

    /// Complete a forechannel request and store the encoded Compound4Res body
    /// for future retries on the same slot/sequence.
    ///
    /// The token is borrowed rather than consumed because this call must be
    /// cancellation-safe: awaiting the state lock is its only cancellation
    /// point, and `token` is still `Some` if the future is dropped there, so
    /// the caller keeps ownership of the slot and can install a fallback reply.
    /// A slot that could not be finalized hands the token back for the same
    /// reason.
    pub(crate) async fn finish_sequence(
        &self,
        token: &mut Option<SequenceCacheToken>,
        response: Vec<u8>,
    ) -> Result<(), NfsStat4> {
        let mut inner = self.inner.write().await;
        // Everything below runs to completion without awaiting, so the slot
        // cannot be left half-finalized once the token has been taken.
        let Some(taken) = token.take() else {
            return Ok(());
        };
        match Self::finish_sequence_locked(&mut inner, taken, response) {
            Ok(()) => Ok(()),
            Err((status, taken)) => {
                *token = Some(taken);
                Err(status)
            }
        }
    }

    /// Complete a forechannel request without awaiting the state lock.
    ///
    /// This exists for the panic/cancellation cleanup path, which runs inside
    /// `Drop` and therefore cannot await. A contended lock hands the token and
    /// response body back so the caller can retry them asynchronously; a slot
    /// that could not be finalized at all reports its status instead, so the
    /// caller does not mistake the failure for a cached reply.
    pub(crate) fn try_finish_sequence(
        &self,
        token: SequenceCacheToken,
        response: Vec<u8>,
    ) -> TryFinishSequence {
        match self.inner.try_write() {
            Ok(mut inner) => match Self::finish_sequence_locked(&mut inner, token, response) {
                Ok(()) => TryFinishSequence::Finished,
                Err((status, _token)) => TryFinishSequence::Failed(status),
            },
            Err(_) => TryFinishSequence::Contended(token, response),
        }
    }

    /// Stores the reply, or hands the token back with the status that stopped
    /// it so the caller can decide on a fallback.
    fn finish_sequence_locked(
        inner: &mut StateInner,
        token: SequenceCacheToken,
        response: Vec<u8>,
    ) -> Result<(), (NfsStat4, SequenceCacheToken)> {
        let Some(session) = inner.sessions.get_mut(&token.sessionid) else {
            return Err((NfsStat4::BadSession, token));
        };
        let slot_idx = token.slotid as usize;
        let Some(slot) = session.slots.get_mut(slot_idx) else {
            return Err((NfsStat4::BadSlot, token));
        };

        slot.in_progress = None;
        slot.cached_reply = Some(CachedReplay {
            fingerprint: token.fingerprint,
            response,
        });
        Ok(())
    }
}
