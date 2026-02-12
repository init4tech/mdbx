use crate::{
    error::{MdbxResult, mdbx_result},
    sys::EnvPtr,
};
use std::{
    ptr,
    sync::mpsc::{Receiver, SyncSender, sync_channel},
};

#[derive(Copy, Clone, Debug)]
pub(crate) struct RawTxPtr(pub(crate) *mut ffi::MDBX_txn);

unsafe impl Send for RawTxPtr {}
unsafe impl Sync for RawTxPtr {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitLatencyPtr(pub(crate) *mut ffi::MDBX_commit_latency);

unsafe impl Send for CommitLatencyPtr {}
unsafe impl Sync for CommitLatencyPtr {}

/// Begin transaction request
pub(crate) struct Begin {
    pub(crate) parent: RawTxPtr,
    pub(crate) flags: ffi::MDBX_txn_flags_t,
    pub(crate) sender: SyncSender<MdbxResult<RawTxPtr>>,
    pub(crate) span: tracing::Span,
}

/// Abort transaction request
pub(crate) struct Abort {
    pub(crate) tx: RawTxPtr,
    pub(crate) sender: SyncSender<MdbxResult<bool>>,
    pub(crate) span: tracing::Span,
}

/// Commit transaction request
pub(crate) struct Commit {
    pub(crate) tx: RawTxPtr,
    pub(crate) latency: CommitLatencyPtr,
    pub(crate) sender: SyncSender<MdbxResult<bool>>,
    pub(crate) span: tracing::Span,
}

/// Messages sent to the [`LifecycleHandle`].
pub(crate) enum LifecycleEvent {
    Begin(Begin),
    Abort(Abort),
    Commit(Commit),
}

impl From<Begin> for LifecycleEvent {
    fn from(begin: Begin) -> Self {
        LifecycleEvent::Begin(begin)
    }
}

impl From<Abort> for LifecycleEvent {
    fn from(abort: Abort) -> Self {
        LifecycleEvent::Abort(abort)
    }
}

impl From<Commit> for LifecycleEvent {
    fn from(commit: Commit) -> Self {
        LifecycleEvent::Commit(commit)
    }
}

/// Handle to communicate with the transaction manager.
pub(crate) struct LifecycleHandle {
    sender: SyncSender<LifecycleEvent>,
}

impl LifecycleHandle {
    /// Sends a message to the transaction manager.
    #[track_caller]
    #[inline(always)]
    pub(crate) fn send<T: Into<LifecycleEvent>>(&self, msg: T) {
        self.sender.send(msg.into()).unwrap();
    }
}

impl From<SyncSender<LifecycleEvent>> for LifecycleHandle {
    fn from(sender: SyncSender<LifecycleEvent>) -> Self {
        Self { sender }
    }
}

/// Manages RW transactions in a background thread.
///
/// MDBX requires that RW transactions are committed and aborted
/// from the same thread that created them. This struct spawns a
/// background thread to handle these operations for Sync RW transactions.
#[derive(Debug)]
pub(crate) struct RwSyncLifecycle {
    env: EnvPtr,
    rx: Receiver<LifecycleEvent>,
}

impl RwSyncLifecycle {
    /// Creates a new [`LifecycleHandle`], spawns a background task, returns
    /// a sender to communicate with it.
    pub(crate) fn spawn(env: EnvPtr) -> LifecycleHandle {
        let (tx, rx) = sync_channel(0);
        let txn_manager = Self { env, rx };

        txn_manager.start_message_listener();

        tx.into()
    }

    /// Begin a RW transaction.
    fn handle_begin(&self, Begin { parent, flags, sender, span }: Begin) {
        let _guard = span.entered();
        let mut txn: *mut ffi::MDBX_txn = ptr::null_mut();
        let res = mdbx_result(unsafe {
            ffi::mdbx_txn_begin_ex(self.env.0, parent.0, flags, &mut txn, ptr::null_mut())
        })
        .map(|_| RawTxPtr(txn));
        sender.send(res).unwrap();
    }

    // Abort a transaction.
    fn handle_abort(&self, Abort { tx, sender, span }: Abort) {
        let _guard = span.entered();
        sender.send(mdbx_result(unsafe { ffi::mdbx_txn_abort(tx.0) })).unwrap();
    }

    /// Commit a transaction.
    fn handle_commit(&self, Commit { tx, sender, latency, span }: Commit) {
        let _guard = span.entered();
        sender.send(mdbx_result(unsafe { ffi::mdbx_txn_commit_ex(tx.0, latency.0) })).unwrap();
    }

    /// Spawns a new [`std::thread`] that listens to incoming [`LifecycleEvent`] messages,
    /// executes an FFI function, and returns the result on the provided channel.
    ///
    /// - [`LifecycleEvent::Begin`] opens a new transaction with [`ffi::mdbx_txn_begin_ex`]
    /// - [`LifecycleEvent::Abort`] aborts a transaction with [`ffi::mdbx_txn_abort`]
    /// - [`LifecycleEvent::Commit`] commits a transaction with [`ffi::mdbx_txn_commit_ex`]
    fn start_message_listener(self) {
        let task = move || {
            loop {
                match self.rx.recv() {
                    Ok(msg) => match msg {
                        LifecycleEvent::Begin(begin) => {
                            self.handle_begin(begin);
                        }
                        LifecycleEvent::Abort(abort) => {
                            self.handle_abort(abort);
                        }
                        LifecycleEvent::Commit(commit) => {
                            self.handle_commit(commit);
                        }
                    },
                    Err(_) => return,
                }
            }
        };
        std::thread::Builder::new().name("mdbx-rs-txn-manager".to_string()).spawn(task).unwrap();
    }
}
