//! RFC 5531 record framing for RPC over TCP.
//!
//! The reader and the writer live on opposite halves of the socket and never
//! touch the other half, which is what keeps outbound records intact when
//! several workers complete out of order.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tracing::warn;

use crate::server::{
    CONN_BUF_SIZE, MAX_FRAGMENT_SIZE, MAX_FRAGMENT_SIZE_U32, RPC_FRAG_LEN_MASK, RPC_LAST_FRAGMENT,
};

/// Reassembles inbound RPC records from the read half of a connection.
pub(super) struct RecordReader<R> {
    reader: R,
}

impl<R: AsyncRead + Unpin> RecordReader<R> {
    pub(super) fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Reads one complete RPC record.
    ///
    /// Returns `Ok(None)` when the peer closed the connection cleanly or the
    /// record violates the framing limits, in which case the connection must be
    /// torn down without a reply.
    ///
    /// Not cancellation-safe: a dropped call loses the fragments it had already
    /// reassembled, so the caller may only abandon it when it is tearing the
    /// connection down.
    #[expect(
        clippy::indexing_slicing,
        reason = "fragment lengths are validated against MAX_FRAGMENT_SIZE before slicing"
    )]
    pub(super) async fn read_record(&mut self) -> std::io::Result<Option<Bytes>> {
        let mut record = BytesMut::with_capacity(CONN_BUF_SIZE);

        loop {
            let mut header = [0u8; 4];
            match self.reader.read_exact(&mut header).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            }
            let header_val = u32::from_be_bytes(header);
            let last_fragment = (header_val & RPC_LAST_FRAGMENT) != 0;
            let frag_len = (header_val & RPC_FRAG_LEN_MASK) as usize;

            if frag_len > MAX_FRAGMENT_SIZE {
                warn!("Fragment too large: {frag_len}");
                return Ok(None);
            }

            let record_len = record.len();
            let new_len = match record_len.checked_add(frag_len) {
                Some(len) if len <= MAX_FRAGMENT_SIZE => len,
                _ => {
                    warn!(
                        "RPC record exceeds configured limit: current={}, incoming={}",
                        record_len, frag_len
                    );
                    return Ok(None);
                }
            };
            record.resize(new_len, 0);
            let _ = self
                .reader
                .read_exact(&mut record[record_len..new_len])
                .await?;

            if last_fragment {
                return Ok(Some(record.freeze()));
            }
        }
    }
}

/// A finished reply on its way to the writer, carrying the connection capacity
/// permit that admitted the record it answers.
///
/// One permit covers a request's entire lifecycle — reading its record,
/// executing it, and holding its encoded reply until the socket has taken it —
/// so a connection never holds more than `max_concurrent_requests` request and
/// reply bodies together. Dropping this value is what returns the permit, which
/// also means a discarded queue (a dead writer, a dead connection) can never
/// leak capacity.
pub(super) struct QueuedResponse {
    body: Bytes,
    permit: OwnedSemaphorePermit,
}

impl QueuedResponse {
    pub(super) fn new(body: Bytes, permit: OwnedSemaphorePermit) -> Self {
        Self { body, permit }
    }
}

/// Owns the write half and serializes every encoded reply onto the wire.
///
/// Workers publish complete replies through `responses`; this task fragments
/// them one record at a time, so fragments of different replies never
/// interleave regardless of completion order.
pub(super) async fn response_writer<W: AsyncWrite + Unpin>(
    write_half: W,
    mut responses: mpsc::Receiver<QueuedResponse>,
) -> std::io::Result<()> {
    let mut writer = BufWriter::with_capacity(CONN_BUF_SIZE, write_half);
    // Capacity permits of replies that are encoded into the buffer but not yet
    // on the socket. There are only `max_concurrent_requests` permits in
    // existence, so this holds at most that many.
    let mut buffered = Vec::new();

    while let Some(response) = responses.recv().await {
        let QueuedResponse { body, permit } = response;
        write_record(&mut writer, body).await?;
        buffered.push(permit);
        // Coalesce syscalls when several workers finish together, but never
        // leave a completed reply sitting in the buffer. Emptying the queue is
        // also what returns capacity, and it always happens: once every permit
        // is buffered here no worker can be running, so no further reply can be
        // queued and this branch is taken.
        if responses.is_empty() {
            writer.flush().await?;
            buffered.clear();
        }
    }

    writer.flush().await
}

async fn write_record<W: AsyncWrite + Unpin>(
    writer: &mut W,
    mut response: Bytes,
) -> std::io::Result<()> {
    loop {
        let fragment = response.split_to(response.len().min(MAX_FRAGMENT_SIZE));
        let last_fragment = response.is_empty();
        // The split above bounds the fragment by MAX_FRAGMENT_SIZE, which is
        // well below the RFC 5531 fragment length field limit.
        let mut header = u32::try_from(fragment.len()).unwrap_or(MAX_FRAGMENT_SIZE_U32);
        if last_fragment {
            header |= RPC_LAST_FRAGMENT;
        }
        writer.write_all(&header.to_be_bytes()).await?;
        writer.write_all(&fragment).await?;
        if last_fragment {
            return Ok(());
        }
    }
}
