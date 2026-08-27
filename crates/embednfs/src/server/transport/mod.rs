//! RPC-over-TCP connection handling.
//!
//! Each connection is served by three cooperating parts:
//!
//! * one **record reader** that owns the read half and frames RFC 5531 records;
//! * up to `max_concurrent_requests` **request workers**, one spawned task per
//!   record, which decode and execute the COMPOUND;
//! * one **response writer** that owns the write half and is the only place
//!   that touches the socket for output.
//!
//! Because a single task writes, RPC fragments of different replies can never
//! interleave; replies may be written in any order and are correlated by XID.
//!
//! See `docs/concurrency.md` for the concurrency and cancellation contract.

use std::sync::Arc;

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc};
use tracing::warn;

use crate::fs::FileSystem;

use super::NfsServer;

mod dispatch;
mod record;
mod replay;

use dispatch::ControlGate;
use record::{RecordReader, response_writer};

impl<F: FileSystem> NfsServer<F> {
    /// Serves one accepted TCP connection until the peer disconnects or the
    /// record stream becomes unusable.
    pub(super) async fn handle_connection(
        self: &std::sync::Arc<Self>,
        stream: TcpStream,
    ) -> std::io::Result<()> {
        let connection_id = self.state.alloc_connection_id();
        let limit = self.max_concurrent_requests;
        let (read_half, write_half) = stream.into_split();

        let (responses_tx, responses_rx) = mpsc::channel::<Bytes>(limit);
        let writer = tokio::spawn(response_writer(write_half, responses_rx));

        let capacity = Arc::new(Semaphore::new(limit));
        let control = Arc::new(ControlGate::new());
        let mut reader = RecordReader::new(read_half);

        let read_result = loop {
            // Capacity is taken *before* the next record is read so that a
            // connection can never accumulate unbounded request bodies: an
            // unread record stays in the socket receive buffer and TCP flow
            // control pushes back on the client. The permit is handed to the
            // worker and released only once its response has been published,
            // which bounds queued response bodies the same way.
            let Ok(permit) = Arc::clone(&capacity).acquire_owned().await else {
                break Ok(());
            };

            let record = match reader.read_record().await {
                Ok(Some(record)) => record,
                Ok(None) => break Ok(()),
                Err(e) => break Err(e),
            };

            let Some((call, body)) = Self::decode_rpc_call(record) else {
                break Ok(());
            };

            let server = Arc::clone(self);
            let responses = responses_tx.clone();
            let control = Arc::clone(&control);
            std::mem::drop(tokio::spawn(async move {
                let _permit = permit;
                let response = server
                    .process_rpc_call(call, body, connection_id, &control)
                    .await;
                // A send failure means the peer is gone or the writer failed.
                // The worker has already finalized its replay cache entry at
                // this point, so dropping the encoded reply is safe: the client
                // gets it from the slot's replay cache when it retries.
                let _ = responses.send(response).await;
            }));
        };

        // Dropping the reader's sender lets the writer finish once every worker
        // has dropped its clone, so this also waits for dispatched requests to
        // complete execution and finalize their replay entries before the
        // connection task exits.
        std::mem::drop(responses_tx);
        let write_result = match writer.await {
            Ok(result) => result,
            Err(e) => {
                warn!("Response writer task failed: {e}");
                Ok(())
            }
        };

        read_result.and(write_result)
    }
}
