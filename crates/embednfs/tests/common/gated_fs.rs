//! A `MemFs` wrapper whose `getattr` can be blocked or made to panic, used to
//! observe how the server schedules concurrent forechannel requests.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{Notify, watch};

use embednfs::{
    AccessMask, Attrs, CommitSupport, CreateKind, CreateRequest, CreateResult, DirPage, FileSystem,
    FsResult, FsStats, HardLinks, MemFs, ReadResult, RequestContext, SetAttrs, Symlinks,
    WriteResult, WriteStability, Xattrs,
};

/// Deterministic gate shared between a test and the filesystem backend.
pub struct FsGate {
    blocked: Mutex<HashSet<u64>>,
    panicking: Mutex<HashSet<u64>>,
    entered: AtomicUsize,
    entered_notify: Notify,
    inflight: AtomicUsize,
    max_inflight: AtomicUsize,
    getattr_calls: AtomicUsize,
    release_tx: watch::Sender<bool>,
    release_rx: watch::Receiver<bool>,
}

impl FsGate {
    pub fn new() -> Arc<Self> {
        let (release_tx, release_rx) = watch::channel(false);
        Arc::new(Self {
            blocked: Mutex::new(HashSet::new()),
            panicking: Mutex::new(HashSet::new()),
            entered: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            inflight: AtomicUsize::new(0),
            max_inflight: AtomicUsize::new(0),
            getattr_calls: AtomicUsize::new(0),
            release_tx,
            release_rx,
        })
    }

    /// Number of `getattr` calls that have reached the gate.
    pub fn entered(&self) -> usize {
        self.entered.load(Ordering::SeqCst)
    }

    /// Highest number of `getattr` calls that were inside the gate at once.
    pub fn max_inflight(&self) -> usize {
        self.max_inflight.load(Ordering::SeqCst)
    }

    /// Total number of `getattr` calls the backend has seen, gated or not.
    /// Used to prove that a replayed reply did not re-execute the request.
    pub fn getattr_calls(&self) -> usize {
        self.getattr_calls.load(Ordering::SeqCst)
    }

    /// Resolves once at least `count` calls have reached the gate.
    pub async fn wait_entered(&self, count: usize) {
        loop {
            let notified = self.entered_notify.notified();
            if self.entered() >= count {
                return;
            }
            notified.await;
        }
    }

    /// Releases every current and future waiter.
    pub fn release(&self) {
        let _ = self.release_tx.send(true);
    }

    fn enter(&self) {
        let inflight = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self
            .max_inflight
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |max| {
                Some(max.max(inflight))
            });
        let _ = self.entered.fetch_add(1, Ordering::SeqCst);
        self.entered_notify.notify_waiters();
    }

    fn leave(&self) {
        let _ = self.inflight.fetch_sub(1, Ordering::SeqCst);
    }

    async fn wait_release(&self) {
        let mut release = self.release_rx.clone();
        let _ = release.wait_for(|released| *released).await;
    }

    fn is_blocked(&self, handle: u64) -> bool {
        self.blocked.lock().unwrap().contains(&handle)
    }

    fn should_panic(&self, handle: u64) -> bool {
        self.panicking.lock().unwrap().contains(&handle)
    }
}

/// `MemFs` with a gated `getattr`.
pub struct GatedFs {
    pub inner: MemFs,
    pub gate: Arc<FsGate>,
}

/// Builds a filesystem with the given `(name, size)` files, where `getattr` on
/// any file in `blocked` waits for [`FsGate::release`] and `getattr` on any file
/// in `panicking` panics.
pub async fn gated_fs(
    files: &[(&str, usize)],
    blocked: &[&str],
    panicking: &[&str],
) -> (GatedFs, Arc<FsGate>) {
    let inner = MemFs::new();
    let ctx = RequestContext::anonymous();
    let gate = FsGate::new();

    for (name, size) in files {
        let handle = inner
            .create(
                &ctx,
                &1,
                name,
                CreateRequest {
                    kind: CreateKind::File,
                    attrs: SetAttrs::default(),
                },
            )
            .await
            .unwrap()
            .handle;
        if *size > 0 {
            let _ = inner
                .write(
                    &ctx,
                    &handle,
                    0,
                    Bytes::from(vec![0x5a; *size]),
                    WriteStability::FileSync,
                )
                .await
                .unwrap();
        }
        if blocked.contains(name) {
            let _ = gate.blocked.lock().unwrap().insert(handle);
        }
        if panicking.contains(name) {
            let _ = gate.panicking.lock().unwrap().insert(handle);
        }
    }

    let fs = GatedFs {
        inner,
        gate: Arc::clone(&gate),
    };
    (fs, gate)
}

#[async_trait::async_trait]
impl FileSystem for GatedFs {
    type Handle = u64;

    fn root(&self) -> Self::Handle {
        self.inner.root()
    }
    fn capabilities(&self) -> embednfs::FsCapabilities {
        self.inner.capabilities()
    }
    fn limits(&self) -> embednfs::FsLimits {
        self.inner.limits()
    }
    async fn statfs(&self, ctx: &RequestContext) -> FsResult<FsStats> {
        self.inner.statfs(ctx).await
    }
    async fn getattr(&self, ctx: &RequestContext, handle: &Self::Handle) -> FsResult<Attrs> {
        let _ = self.gate.getattr_calls.fetch_add(1, Ordering::SeqCst);
        if self.gate.should_panic(*handle) {
            panic!("injected filesystem panic for handle {handle}");
        }
        if self.gate.is_blocked(*handle) {
            self.gate.enter();
            self.gate.wait_release().await;
            self.gate.leave();
        }
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
