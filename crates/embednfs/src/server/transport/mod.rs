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

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc};
use tracing::warn;

use crate::fs::FileSystem;

use super::NfsServer;

mod dispatch;
mod record;
mod replay;

#[cfg(test)]
mod tests;

use dispatch::ControlGate;
use record::{QueuedResponse, RecordReader, response_writer};

impl<F: FileSystem> NfsServer<F> {
    /// Serves one accepted TCP connection until the peer disconnects or the
    /// record stream becomes unusable.
    pub(super) async fn handle_connection(
        self: &std::sync::Arc<Self>,
        stream: TcpStream,
    ) -> std::io::Result<()> {
        let (read_half, write_half) = stream.into_split();
        self.serve_halves(read_half, write_half).await
    }

    /// Runs the reader/worker/writer trio over one already split byte stream.
    ///
    /// A TCP connection is the only production caller; keeping the loop generic
    /// over the two halves also lets it be driven over in-memory streams whose
    /// failure points are exact.
    async fn serve_halves<R, W>(
        self: &std::sync::Arc<Self>,
        read_half: R,
        write_half: W,
    ) -> std::io::Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let connection_id = self.state.alloc_connection_id();
        let limit = self.max_concurrent_requests;

        let (responses_tx, responses_rx) = mpsc::channel::<QueuedResponse>(limit);
        let mut writer = tokio::spawn(response_writer(write_half, responses_rx));

        let capacity = Arc::new(Semaphore::new(limit));
        let control = Arc::new(ControlGate::new());
        let mut reader = RecordReader::new(read_half);
        // Set when the writer is what ended the loop, so it is not awaited
        // twice below.
        let mut writer_result = None;

        let read_result = loop {
            // Capacity is taken *before* the next record is read so that a
            // connection can never accumulate unbounded request bodies: an
            // unread record stays in the socket receive buffer and TCP flow
            // control pushes back on the client. The permit then follows the
            // request all the way to the writer, so the same budget bounds
            // request bodies, running workers, and queued replies together.
            let Ok(permit) = Arc::clone(&capacity).acquire_owned().await else {
                break Ok(());
            };

            let record = tokio::select! {
                biased;
                // The writer is watched while the next record is being read.
                // Once the write half is broken every further reply is
                // undeliverable, so the connection stops here instead of
                // executing requests — and committing their side effects —
                // that the client can never be told the outcome of. Abandoning
                // the half-read record is safe precisely because this path
                // tears the connection down.
                result = &mut writer => {
                    writer_result = Some(result);
                    break Ok(());
                }
                record = reader.read_record() => match record {
                    Ok(Some(record)) => record,
                    Ok(None) => break Ok(()),
                    Err(e) => break Err(e),
                },
            };

            let Some((call, body)) = Self::decode_rpc_call(record) else {
                break Ok(());
            };

            let server = Arc::clone(self);
            let responses = responses_tx.clone();
            let control = Arc::clone(&control);
            std::mem::drop(tokio::spawn(async move {
                let response = server
                    .process_rpc_call(call, body, connection_id, &control)
                    .await;
                // The permit rides along with the reply and is released by the
                // writer, never by the queue insert. A send failure means the
                // peer is gone or the writer failed; the worker has already
                // finalized its replay cache entry at this point, so dropping
                // the encoded reply — and with it the permit — is safe: the
                // client gets the reply from the slot's replay cache when it
                // retries.
                let _ = responses.send(QueuedResponse::new(response, permit)).await;
            }));
        };

        // Dropping the reader's sender lets the writer finish once every worker
        // has dropped its clone.
        std::mem::drop(responses_tx);

        // Every permit is held by a running worker, by a queued reply, or by a
        // reply the writer has buffered, so reclaiming the whole capacity waits
        // for every already dispatched request to finish executing and finalize
        // its replay cache entry. Unlike awaiting the writer, this still holds
        // when the writer is the part that died: it drops the receiver, which
        // drops the queued replies and returns their permits.
        let permits = u32::try_from(limit).unwrap_or(u32::MAX);
        let _drained = capacity.acquire_many(permits).await;

        // A writer that already ended the loop must not be awaited again.
        let writer_result = match writer_result {
            Some(result) => result,
            None => writer.await,
        };
        let write_result = match writer_result {
            Ok(result) => result,
            Err(e) => {
                warn!("Response writer task failed: {e}");
                Ok(())
            }
        };

        read_result.and(write_result)
    }
}
