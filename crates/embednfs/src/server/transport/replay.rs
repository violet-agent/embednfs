//! Fail-safe finalization of a prepared forechannel slot.
//!
//! Between `prepare_sequence` and `finish_sequence` a slot is marked
//! in-progress: retries get NFS4ERR_DELAY and the slot has no replayable reply.
//! A worker that dies in that window (a panic in a filesystem backend, or task
//! cancellation at runtime shutdown) must not leave the slot in that state, and
//! must not simply clear `in_progress` either — the request may already have
//! performed side effects, so making the slot reusable would invite duplicate
//! execution of a mutating COMPOUND.
//!
//! Instead the drop path finalizes the slot with a replayable
//! NFS4ERR_SERVERFAULT COMPOUND: the sequence id stays consumed, a retry with
//! the same arguments replays the fault instead of re-executing, and a
//! different request on that slot still gets NFS4ERR_SEQ_FALSE_RETRY.

use std::sync::Arc;

use bytes::BytesMut;
use embednfs_proto::NfsStat4;
use embednfs_proto::xdr::XdrEncode;
use tracing::{error, warn};

use crate::session::{SequenceCacheToken, StateManager};

use crate::server::compound::sequence_error_compound;

/// Guards the in-progress window of one prepared slot.
pub(super) struct SequenceFinalizer {
    state: Arc<StateManager>,
    /// `None` once the slot has been finalized with a real reply.
    token: Option<SequenceCacheToken>,
    tag: String,
}

impl SequenceFinalizer {
    pub(super) fn new(state: Arc<StateManager>, token: SequenceCacheToken, tag: String) -> Self {
        Self {
            state,
            token: Some(token),
            tag,
        }
    }

    /// Caches the final encoded `Compound4Res` body for the slot.
    ///
    /// Must be called before the reply is published to the response writer so
    /// that a retry never races ahead of the replay cache entry.
    pub(super) async fn finish(mut self, body: Vec<u8>) {
        let Some(token) = self.token.take() else {
            return;
        };
        if let Err(status) = self.state.finish_sequence(token, body).await {
            warn!("Failed to finalize replay cache entry: {status:?}");
        }
    }

    fn fault_body(tag: &str) -> Vec<u8> {
        let mut body = BytesMut::with_capacity(64);
        sequence_error_compound(tag, NfsStat4::Serverfault).encode(&mut body);
        body.to_vec()
    }
}

impl Drop for SequenceFinalizer {
    fn drop(&mut self) {
        let Some(token) = self.token.take() else {
            return;
        };
        warn!(
            "Forechannel worker dropped after prepare_sequence (slot {}); caching a replayable SERVERFAULT reply",
            token.slotid
        );

        let body = Self::fault_body(&self.tag);
        let state = Arc::clone(&self.state);
        let Err((token, body)) = state.try_finish_sequence(token, body) else {
            return;
        };

        // The state lock was contended, and `Drop` cannot await. Hand the
        // finalization to the runtime instead.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => std::mem::drop(handle.spawn(async move {
                if let Err(status) = state.finish_sequence(token, body).await {
                    warn!("Failed to finalize faulted replay cache entry: {status:?}");
                }
            })),
            Err(_) => error!(
                "No runtime available to finalize faulted slot {}; slot stays in progress until the session is destroyed or its lease expires",
                token.slotid
            ),
        }
    }
}
