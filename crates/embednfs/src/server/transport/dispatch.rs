//! Per-request worker path: decode, lane selection, SEQUENCE handling, and
//! COMPOUND execution.

use bytes::{Bytes, BytesMut};
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{trace, warn};

use embednfs_proto::xdr::*;
use embednfs_proto::*;

use crate::fs::FileSystem;
use crate::session::SequenceReplay;

use super::replay::SequenceFinalizer;
use crate::server::compound::{sequence_error_compound, sequence_only_compound};
use crate::server::{
    Compound4Res, MAX_CONCURRENT_REQUESTS_LIMIT, NfsServer, hex_bytes, replay_fingerprint,
};

/// Serializes control traffic against forechannel traffic on one connection.
///
/// COMPOUNDs that start with a valid SEQUENCE take the gate in shared mode and
/// therefore run concurrently with each other; session and other unsequenced
/// control COMPOUNDs (EXCHANGE_ID, CREATE_SESSION, DESTROY_SESSION,
/// DESTROY_CLIENTID, BIND_CONN_TO_SESSION, and rejected unsequenced requests)
/// take it exclusively. Session creation and destruction therefore keep the
/// conservative "nothing else is running" behavior they had when requests were
/// executed one at a time.
pub(super) struct ControlGate {
    /// A shared holder takes one permit; an exclusive holder takes them all.
    /// `tokio`'s semaphore is FIFO-fair, so a waiting exclusive holder is not
    /// starved by a stream of shared holders.
    permits: Semaphore,
}

impl ControlGate {
    pub(super) fn new() -> Self {
        Self {
            permits: Semaphore::new(MAX_CONCURRENT_REQUESTS_LIMIT),
        }
    }

    /// Acquires the lane for one request. The returned permit must be held for
    /// the whole request.
    ///
    /// Returns `None` only if the semaphore were closed, which never happens:
    /// the gate lives as long as the connection and is never closed.
    async fn acquire(&self, shared: bool) -> Option<SemaphorePermit<'_>> {
        let wanted = if shared {
            1
        } else {
            u32::try_from(MAX_CONCURRENT_REQUESTS_LIMIT).unwrap_or(u32::MAX)
        };
        self.permits.acquire_many(wanted).await.ok()
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "body_start is captured from the pre-encode length and response only grows afterward"
)]
fn replay_cache_body(response: &BytesMut, body_start: usize) -> Vec<u8> {
    response[body_start..].to_vec()
}

impl<F: FileSystem> NfsServer<F> {
    /// Decodes the RPC call header of a freshly framed record.
    ///
    /// Returns `None` when the header is malformed; as before, such a record
    /// cannot be answered (there is no trustworthy XID) and the connection is
    /// closed by the caller.
    pub(super) fn decode_rpc_call(record: Bytes) -> Option<(RpcCallHeader, Bytes)> {
        trace!(
            "RPC request bytes={} hex={}",
            record.len(),
            hex_bytes(&record)
        );
        let mut src = record;
        match RpcCallHeader::decode(&mut src) {
            Ok(call) => Some((call, src)),
            Err(e) => {
                warn!("Failed to decode RPC header: {e}");
                None
            }
        }
    }

    /// Executes one RPC call and returns its complete encoded reply.
    pub(super) async fn process_rpc_call(
        &self,
        call: RpcCallHeader,
        body: Bytes,
        connection_id: u64,
        control: &ControlGate,
    ) -> Bytes {
        let mut response = BytesMut::with_capacity(8192);

        if call.rpcvers != RPC_VERSION {
            encode_rpc_reply_prog_mismatch(&mut response, call.xid, RPC_VERSION, RPC_VERSION);
            return response.freeze();
        }

        if call.prog != NFS_PROGRAM {
            encode_rpc_reply_prog_mismatch(&mut response, call.xid, NFS_PROGRAM, NFS_PROGRAM);
            return response.freeze();
        }

        if call.vers != NFS_V4 {
            encode_rpc_reply_prog_mismatch(&mut response, call.xid, NFS_V4, NFS_V4);
            return response.freeze();
        }

        if let Err(auth) = Self::validate_rpc_auth(&call) {
            encode_rpc_reply_auth_error(&mut response, call.xid, auth);
            return response.freeze();
        }

        match call.proc_num {
            0 => encode_rpc_reply_accepted(&mut response, call.xid),
            1 => {
                self.process_compound_call(&call, body, connection_id, control, &mut response)
                    .await;
            }
            _ => encode_rpc_reply_proc_unavail(&mut response, call.xid),
        }

        let response = response.freeze();
        trace!(
            "RPC response xid={} bytes={} hex={}",
            call.xid,
            response.len(),
            hex_bytes(&response)
        );
        response
    }

    async fn process_compound_call(
        &self,
        call: &RpcCallHeader,
        body: Bytes,
        connection_id: u64,
        control: &ControlGate,
        response: &mut BytesMut,
    ) {
        let compound_payload = body.clone();
        let mut src = body;
        let args = match Compound4Args::decode(&mut src) {
            Ok(args) => args,
            Err(e) => {
                warn!("Failed to decode COMPOUND: {e}");
                encode_rpc_reply_accepted(response, call.xid);
                Compound4Res {
                    status: NfsStat4::BadXdr,
                    tag: String::new(),
                    resarray: vec![],
                }
                .encode(response);
                return;
            }
        };

        let leading_sequence =
            args.minorversion == 1 && matches!(args.argarray.first(), Some(NfsArgop4::Sequence(_)));
        // Held for the whole request; see `ControlGate`.
        let _lane = control.acquire(leading_sequence).await;

        let request_ctx = Self::request_context(&call.cred);
        let mut finalizer = None;
        let prepared_sequence = match args.argarray.first() {
            Some(NfsArgop4::Sequence(seq_args)) if leading_sequence => {
                let fingerprint = replay_fingerprint(&call.cred, &compound_payload);
                match self
                    .state
                    .prepare_sequence(seq_args, &fingerprint, connection_id)
                    .await
                {
                    SequenceReplay::Execute(res, token) => {
                        finalizer = Some(SequenceFinalizer::new(
                            std::sync::Arc::clone(&self.state),
                            token,
                            args.tag.clone(),
                        ));
                        Some(NfsResop4::Sequence(NfsStat4::Ok, Some(res)))
                    }
                    SequenceReplay::Replay(cached) => {
                        encode_rpc_reply_accepted(response, call.xid);
                        response.extend_from_slice(&cached);
                        return;
                    }
                    SequenceReplay::StatusOnly(res) => {
                        let result = sequence_only_compound(&args.tag, res);
                        encode_rpc_reply_accepted(response, call.xid);
                        result.encode(response);
                        return;
                    }
                    SequenceReplay::Error(status) => {
                        let result = sequence_error_compound(&args.tag, status);
                        encode_rpc_reply_accepted(response, call.xid);
                        result.encode(response);
                        return;
                    }
                }
            }
            _ => None,
        };

        let result = self
            .handle_compound(args, prepared_sequence, &request_ctx, connection_id)
            .await;
        encode_rpc_reply_accepted(response, call.xid);
        let body_start = response.len();
        result.encode(response);

        // The slot becomes replayable before the reply is handed to the writer,
        // so a retry can never observe a completed request as in-progress and a
        // disconnect can never lose the executed result.
        if let Some(finalizer) = finalizer {
            finalizer
                .finish(replay_cache_body(response, body_start))
                .await;
        }
    }
}
