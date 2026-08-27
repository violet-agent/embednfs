//! Unit tests for the connection loop and its slot finalization.
//!
//! These live inside the crate because both cases need a failure point a real
//! socket cannot express: one parks a task on the state manager's write lock,
//! the other breaks the write half while the read half keeps delivering.
//! Everything socket-observable is covered by
//! `tests/forechannel_concurrency.rs` instead.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code asserts by panicking; a failed unwrap is a failed test"
)]

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWrite;
use tokio::sync::watch;

use embednfs_proto::xdr::XdrEncode;
use embednfs_proto::{
    Bitmap4, ChannelAttrs4, ClientOwner4, CreateSessionArgs4, EXCHGID4_FLAG_USE_NON_PNFS,
    ExchangeIdArgs4, FATTR4_SIZE, NFS_PROGRAM, NFS_V4, NfsStat4, OP_GETATTR, OP_PUTROOTFH,
    OP_SEQUENCE, OpaqueAuth, SequenceArgs4, Sessionid4, StateProtect4A,
};

use crate::fs::{
    AccessMask, Attrs, CommitSupport, CreateRequest, CreateResult, DirPage, FileSystem,
    FsCapabilities, FsLimits, FsResult, FsStats, HardLinks, ReadResult, RequestContext, SetAttrs,
    Symlinks, WriteResult, WriteStability, Xattrs,
};
use crate::memfs::MemFs;
use crate::server::NfsServer;
use crate::server::compound::sequence_error_compound;
use crate::session::{SequenceReplay, StateManager};

use super::replay::SequenceFinalizer;

/// `MemFs` that counts the `getattr` calls reaching the backend, which is how
/// these tests tell "the request executed" from "the request was never
/// dispatched".
struct ProbeFs {
    inner: MemFs,
    getattr_calls: Arc<AtomicUsize>,
}

impl ProbeFs {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let getattr_calls = Arc::new(AtomicUsize::new(0));
        let fs = Self {
            inner: MemFs::new(),
            getattr_calls: Arc::clone(&getattr_calls),
        };
        (fs, getattr_calls)
    }
}

#[async_trait::async_trait]
impl FileSystem for ProbeFs {
    type Handle = u64;

    fn root(&self) -> Self::Handle {
        self.inner.root()
    }
    fn capabilities(&self) -> FsCapabilities {
        self.inner.capabilities()
    }
    fn limits(&self) -> FsLimits {
        self.inner.limits()
    }
    async fn statfs(&self, ctx: &RequestContext) -> FsResult<FsStats> {
        self.inner.statfs(ctx).await
    }
    async fn getattr(&self, ctx: &RequestContext, handle: &Self::Handle) -> FsResult<Attrs> {
        let _ = self.getattr_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.getattr(ctx, handle).await
    }
    async fn access(
        &self,
        ctx: &RequestContext,
        handle: &Self::Handle,
        requested: AccessMask,
    ) -> FsResult<AccessMask> {
        self.inner.access(ctx, handle, requested).await
    }
    async fn lookup(
        &self,
        ctx: &RequestContext,
        parent: &Self::Handle,
        name: &str,
    ) -> FsResult<Self::Handle> {
        self.inner.lookup(ctx, parent, name).await
    }
    async fn parent(
        &self,
        ctx: &RequestContext,
        dir: &Self::Handle,
    ) -> FsResult<Option<Self::Handle>> {
        self.inner.parent(ctx, dir).await
    }
    async fn readdir(
        &self,
        ctx: &RequestContext,
        dir: &Self::Handle,
        cookie: u64,
        max_entries: u32,
        with_attrs: bool,
    ) -> FsResult<DirPage<Self::Handle>> {
        self.inner
            .readdir(ctx, dir, cookie, max_entries, with_attrs)
            .await
    }
    async fn read(
        &self,
        ctx: &RequestContext,
        handle: &Self::Handle,
        offset: u64,
        count: u32,
    ) -> FsResult<ReadResult> {
        self.inner.read(ctx, handle, offset, count).await
    }
    async fn write(
        &self,
        ctx: &RequestContext,
        handle: &Self::Handle,
        offset: u64,
        data: Bytes,
        requested: WriteStability,
    ) -> FsResult<WriteResult> {
        self.inner.write(ctx, handle, offset, data, requested).await
    }
    async fn create(
        &self,
        ctx: &RequestContext,
        parent: &Self::Handle,
        name: &str,
        req: CreateRequest,
    ) -> FsResult<CreateResult<Self::Handle>> {
        self.inner.create(ctx, parent, name, req).await
    }
    async fn remove(
        &self,
        ctx: &RequestContext,
        parent: &Self::Handle,
        name: &str,
    ) -> FsResult<()> {
        self.inner.remove(ctx, parent, name).await
    }
    async fn rename(
        &self,
        ctx: &RequestContext,
        from_dir: &Self::Handle,
        from_name: &str,
        to_dir: &Self::Handle,
        to_name: &str,
    ) -> FsResult<()> {
        self.inner
            .rename(ctx, from_dir, from_name, to_dir, to_name)
            .await
    }
    async fn setattr(
        &self,
        ctx: &RequestContext,
        handle: &Self::Handle,
        attrs: &SetAttrs,
    ) -> FsResult<Attrs> {
        self.inner.setattr(ctx, handle, attrs).await
    }
    fn symlinks(&self) -> Option<&dyn Symlinks<Self::Handle>> {
        self.inner.symlinks()
    }
    fn hard_links(&self) -> Option<&dyn HardLinks<Self::Handle>> {
        self.inner.hard_links()
    }
    fn xattrs(&self) -> Option<&dyn Xattrs<Self::Handle>> {
        self.inner.xattrs()
    }
    fn commit_support(&self) -> Option<&dyn CommitSupport<Self::Handle>> {
        self.inner.commit_support()
    }
}

/// Write half that fails the first time the writer touches the socket and
/// announces it, so a test can act exactly at the failure point.
struct FailingWriter {
    failed: watch::Sender<bool>,
}

impl AsyncWrite for FailingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Signalled from inside the failing poll. On a current-thread runtime
        // the writer task therefore always reaches its `Err` return, and the
        // connection loop always sees a finished writer, before anything the
        // test does in response can become visible to the reader.
        let _ = self.failed.send(true);
        Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "peer reset")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Creates a confirmed client and session straight in the state manager, which
/// is all these tests need from the EXCHANGE_ID/CREATE_SESSION handshake.
async fn new_session(state: &StateManager) -> Sessionid4 {
    let client = state
        .exchange_id(&ExchangeIdArgs4 {
            clientowner: ClientOwner4 {
                verifier: [0x42; 8],
                ownerid: b"transport-tests".to_vec().into(),
            },
            flags: EXCHGID4_FLAG_USE_NON_PNFS,
            state_protect: StateProtect4A::None,
            client_impl_id: vec![],
        })
        .await
        .unwrap();
    state
        .create_session(
            &CreateSessionArgs4 {
                clientid: client.clientid,
                sequence: client.sequenceid,
                flags: 0,
                fore_chan_attrs: ChannelAttrs4::default(),
                back_chan_attrs: ChannelAttrs4::default(),
                cb_program: 0,
                sec_parms: vec![],
            },
            0,
        )
        .await
        .unwrap()
        .sessionid
}

fn sequence_args(sessionid: Sessionid4, sequenceid: u32, slotid: u32) -> SequenceArgs4 {
    SequenceArgs4 {
        sessionid,
        sequenceid,
        slotid,
        highest_slotid: slotid,
        cachethis: false,
    }
}

/// Encodes `SEQUENCE; PUTROOTFH; GETATTR(size)` — the smallest COMPOUND that
/// takes a slot and reaches [`FileSystem::getattr`].
fn getattr_compound(tag: &str, sessionid: &Sessionid4, sequenceid: u32, slotid: u32) -> Bytes {
    let mut seq = BytesMut::new();
    OP_SEQUENCE.encode(&mut seq);
    seq.put_slice(sessionid);
    sequenceid.encode(&mut seq);
    slotid.encode(&mut seq);
    slotid.encode(&mut seq);
    false.encode(&mut seq);

    let mut getattr = BytesMut::new();
    OP_GETATTR.encode(&mut getattr);
    let mut bitmap = Bitmap4::new();
    bitmap.set(FATTR4_SIZE);
    bitmap.encode(&mut getattr);

    let mut buf = BytesMut::with_capacity(128);
    tag.to_string().encode(&mut buf);
    1u32.encode(&mut buf);
    3u32.encode(&mut buf);
    buf.put_slice(&seq);
    OP_PUTROOTFH.encode(&mut buf);
    buf.put_slice(&getattr);
    buf.freeze()
}

/// Wraps a COMPOUND payload in an RPC call message and one last-fragment
/// RFC 5531 record.
fn rpc_record(xid: u32, compound: &Bytes) -> Bytes {
    let mut msg = BytesMut::with_capacity(compound.len() + 64);
    xid.encode(&mut msg);
    0u32.encode(&mut msg);
    2u32.encode(&mut msg);
    NFS_PROGRAM.encode(&mut msg);
    NFS_V4.encode(&mut msg);
    1u32.encode(&mut msg);
    OpaqueAuth::null().encode(&mut msg);
    OpaqueAuth::null().encode(&mut msg);
    msg.put_slice(compound);

    let mut record = BytesMut::with_capacity(msg.len() + 4);
    let header = u32::try_from(msg.len()).unwrap() | crate::server::RPC_LAST_FRAGMENT;
    header.encode(&mut record);
    record.put_slice(&msg);
    record.freeze()
}

/// The replay fingerprint the server derives from a null-auth call carrying
/// `compound`.
fn fingerprint(compound: &Bytes) -> Vec<u8> {
    crate::server::replay_fingerprint(&OpaqueAuth::null(), compound)
}

/// The encoded body the drop path caches for a slot that produced no reply.
fn fault_body(tag: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    sequence_error_compound(tag, NfsStat4::Serverfault).encode(&mut body);
    body.to_vec()
}

/// Cancelling `SequenceFinalizer::finish` while it waits for the state lock
/// leaves the slot replayable instead of stuck in progress: the finalizer
/// disarms only once the reply is actually cached.
/// Origin: cancellation-safety review of the prepare/finish window.
/// RFC: RFC 8881 §2.10.6.1.3 (slot state after a failed request), §15.1.
#[tokio::test]
async fn test_finish_cancelled_on_the_state_lock_leaves_a_replayable_fault() {
    let state = Arc::new(StateManager::new());
    let sessionid = new_session(&state).await;
    let SequenceReplay::Execute(_, token) = state
        .prepare_sequence(&sequence_args(sessionid, 1, 0), b"cancelled", 1)
        .await
    else {
        panic!("expected a fresh slot to execute");
    };
    let finalizer = SequenceFinalizer::new(Arc::clone(&state), token, "cancel".to_string());

    // Park the worker exactly where a cancelled task would be interrupted:
    // inside `finish`, waiting for the state lock, with the slot in progress
    // and the token not yet handed over.
    let guard = state.lock_state_for_test().await;
    let finishing = tokio::spawn(async move { finalizer.finish(b"executed reply".to_vec()).await });
    tokio::task::yield_now().await;

    finishing.abort();
    assert!(finishing.await.unwrap_err().is_cancelled());

    std::mem::drop(guard);
    // The drop path could not take the contended lock, so it handed the
    // finalization to the runtime; let that task run.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    match state
        .prepare_sequence(&sequence_args(sessionid, 1, 0), b"cancelled", 1)
        .await
    {
        SequenceReplay::Replay(body) => assert_eq!(
            body,
            fault_body("cancel"),
            "the cancelled slot must replay a SERVERFAULT COMPOUND"
        ),
        SequenceReplay::Error(NfsStat4::Delay) => {
            panic!("the slot is still in progress: the cancelled finish leaked it")
        }
        _ => panic!("expected the slot to be replayable"),
    }
}

/// Once a response write fails, the connection stops dispatching records: a
/// request already sitting in the read half never reaches the filesystem, while
/// the worker that was already running still finalizes its replay entry.
/// Origin: writer-failure review of the concurrent forechannel loop.
/// RFC: RFC 8881 §2.10.6.1.3 (executed requests stay replayable), §2.10.6.2.
#[tokio::test]
async fn test_no_record_is_dispatched_after_a_response_write_fails() {
    let (fs, getattr_calls) = ProbeFs::new();
    let server = Arc::new(NfsServer::builder(fs).max_concurrent_requests(1).build());
    let sessionid = new_session(&server.state).await;

    let executed = getattr_compound("executed", &sessionid, 1, 0);
    let refused = getattr_compound("refused", &sessionid, 1, 1);

    let (mut client, read_half) = tokio::io::duplex(4096);
    let (failed_tx, mut failed_rx) = watch::channel(false);
    let write_half = FailingWriter { failed: failed_tx };

    let connection = {
        let server = Arc::clone(&server);
        tokio::spawn(async move { server.serve_halves(read_half, write_half).await })
    };

    tokio::io::AsyncWriteExt::write_all(&mut client, &rpc_record(1, &executed))
        .await
        .unwrap();

    // The second record only becomes readable once the write half is broken,
    // so executing it at all means the connection kept going after the failure.
    // The write itself is best-effort: a connection that reacted to the failure
    // has already dropped its read half, which is exactly the expected outcome.
    failed_rx.changed().await.unwrap();
    let _ = tokio::io::AsyncWriteExt::write_all(&mut client, &rpc_record(2, &refused)).await;
    std::mem::drop(client);

    let result = connection.await.unwrap();
    assert_eq!(
        result.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe,
        "the connection reports the write failure"
    );

    assert_eq!(
        getattr_calls.load(Ordering::SeqCst),
        1,
        "the record read after the write failure must not reach the filesystem"
    );

    // The worker that was already running still finalized its slot, so its
    // executed result survives for a retry on a new connection.
    match server
        .state
        .prepare_sequence(&sequence_args(sessionid, 1, 0), &fingerprint(&executed), 2)
        .await
    {
        SequenceReplay::Replay(body) => {
            assert_ne!(
                body,
                fault_body("executed"),
                "the executed reply, not a fault, is cached"
            );
            assert_eq!(
                body.get(0..4),
                Some(&(NfsStat4::Ok as u32).to_be_bytes()[..]),
                "the cached COMPOUND succeeded"
            );
        }
        _ => panic!("the dispatched worker must leave a replayable reply"),
    }

    // The refused request never even took its slot.
    assert!(
        matches!(
            server
                .state
                .prepare_sequence(&sequence_args(sessionid, 1, 1), &fingerprint(&refused), 2)
                .await,
            SequenceReplay::Execute(_, _)
        ),
        "the slot of the undispatched record must still be untouched"
    );
}
