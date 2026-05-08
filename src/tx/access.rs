use crate::{
    Environment,
    sys::txn_manager::{Abort, RawTxPtr},
    tx::{
        cache::{Cache, DbCache},
        ops,
    },
};
use core::fmt;
use parking_lot::{Mutex, MutexGuard};
use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::sync_channel,
    },
};
use tracing::debug_span;

mod sealed {
    #[allow(unreachable_pub)]
    pub trait Sealed {}
    impl Sealed for super::PtrUnsync {}
    impl Sealed for super::PtrSync {}

    impl<T> Sealed for &T where T: super::TxPtrAccess {}
    impl<T> Sealed for &mut T where T: super::TxPtrAccess {}
    impl<T> Sealed for std::sync::Arc<T> where T: super::TxPtrAccess {}
    impl<T> Sealed for Box<T> where T: super::TxPtrAccess {}
}

/// Trait for accessing the transaction pointer.
///
/// This trait abstracts over the different ways transaction pointers
/// are stored for read-only and read-write transactions. It ensures that
/// the transaction pointer can be accessed safely, respecting timeouts
/// and ownership semantics.
///
/// The associated [`Cache`] type co-locates the cursor cache with the
/// transaction so that the cache lifetime is bound to the transaction
/// pointer's: cached cursors are drained and closed exactly once, in the
/// implementing type's `Drop`, before the transaction is aborted.
#[allow(unreachable_pub)]
pub trait TxPtrAccess: fmt::Debug + sealed::Sealed {
    /// Cache type co-located with this transaction pointer.
    type Cache: Cache;

    /// Create an instance of the implementing type from a raw transaction
    /// pointer.
    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, env: Environment, is_read_only: bool) -> Self
    where
        Self: Sized;

    /// Execute a closure with the transaction pointer.
    fn with_txn_ptr<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R;

    /// Mark the transaction as committed.
    fn mark_committed(&self);

    /// Returns a reference to the cursor/database cache associated with
    /// this transaction.
    fn cache(&self) -> &Self::Cache;

    /// Get the transaction ID by making a call into the MDBX C API.
    fn tx_id(&self) -> Option<usize> {
        let mut id = 0;
        self.with_txn_ptr(|ptr| {
            id = unsafe { ffi::mdbx_txn_id(ptr) as usize };
        });
        // 0 indicates the transaction is not valid
        (id != 0).then_some(id)
    }
}

impl<T> TxPtrAccess for Arc<T>
where
    T: TxPtrAccess,
{
    type Cache = T::Cache;

    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, env: Environment, is_read_only: bool) -> Self
    where
        Self: Sized,
    {
        T::from_ptr_and_env(ptr, env, is_read_only).into()
    }

    fn with_txn_ptr<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        self.as_ref().with_txn_ptr(f)
    }

    fn mark_committed(&self) {
        self.as_ref().mark_committed();
    }

    fn cache(&self) -> &Self::Cache {
        self.as_ref().cache()
    }
}

/// Wrapper for raw txn pointer for unsynchronized transactions.
pub struct PtrUnsync {
    committed: AtomicBool,
    ptr: *mut ffi::MDBX_txn,
    cache: RefCell<DbCache>,
}

impl fmt::Debug for PtrUnsync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PtrUnsync").field("committed", &self.committed).finish()
    }
}

impl TxPtrAccess for PtrUnsync {
    type Cache = RefCell<DbCache>;

    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, _env: Environment, _is_read_only: bool) -> Self
    where
        Self: Sized,
    {
        Self { committed: AtomicBool::new(false), ptr, cache: RefCell::new(DbCache::default()) }
    }

    fn with_txn_ptr<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        f(self.ptr)
    }

    fn mark_committed(&self) {
        // SAFETY:
        // Type is neither Sync nor Send, so no concurrent access is possible.
        unsafe { *self.committed.as_ptr() = true };
    }

    fn cache(&self) -> &Self::Cache {
        &self.cache
    }
}

impl Drop for PtrUnsync {
    fn drop(&mut self) {
        // SAFETY:
        // We have exclusive ownership of this pointer.
        unsafe {
            // Close any cached cursors before the txn ends. No-op if
            // commit/abort already drained.
            for cursor in self.cache.get_mut().drain_cursors() {
                // SAFETY: cursor was returned from an earlier Cursor::drop
                // bound to this still-live txn.
                ops::cursor_close_raw(cursor);
            }
            if !*self.committed.as_ptr() {
                ffi::mdbx_txn_abort(self.ptr);
            }
        }
    }
}

/// A shareable pointer to an MDBX transaction.
///
/// This type is used internally to manage transaction access in the [`TxSync`]
/// transaction API. Users typically don't interact with this type directly.
///
/// [`TxSync`]: crate::tx::aliases::TxSync
#[derive(Debug)]
pub struct PtrSync {
    /// Raw pointer to the MDBX transaction.
    txn: *mut ffi::MDBX_txn,

    /// Whether the transaction was committed.
    committed: AtomicBool,

    /// Lock that serialises access to the transaction pointer **and**
    /// guards the cursor/database cache. Held inside [`with_txn_ptr`]; also
    /// taken implicitly via the [`Cache`] impl on `Mutex<DbCache>`.
    lock: Mutex<DbCache>,

    /// The environment that owns the transaction.
    env: Environment,

    /// Whether the transaction is read-only.
    is_read_only: bool,
}

// SAFETY: Access to the transaction is synchronized by the lock.
unsafe impl Send for PtrSync {}

// SAFETY: Access to the transaction is synchronized by the lock.
unsafe impl Sync for PtrSync {}

impl PtrSync {
    /// Acquires the inner transaction lock to guarantee exclusive access to the transaction
    /// pointer.
    pub(crate) fn lock(&self) -> MutexGuard<'_, DbCache> {
        if let Some(lock) = self.lock.try_lock() {
            lock
        } else {
            tracing::trace!(
                target: "libmdbx",
                txn = %self.txn as usize,
                backtrace = %std::backtrace::Backtrace::capture(),
                "Transaction lock is already acquired, blocking...
                To display the full backtrace, run with `RUST_BACKTRACE=full` env variable."
            );
            self.lock.lock()
        }
    }
}

impl TxPtrAccess for PtrSync {
    type Cache = Mutex<DbCache>;

    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, env: Environment, is_read_only: bool) -> Self
    where
        Self: Sized,
    {
        Self {
            committed: AtomicBool::new(false),
            lock: Mutex::new(DbCache::default()),
            txn: ptr,
            env,
            is_read_only,
        }
    }

    fn with_txn_ptr<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        let _lock = self.lock();
        f(self.txn)
    }

    fn mark_committed(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    fn cache(&self) -> &Self::Cache {
        &self.lock
    }
}

impl Drop for PtrSync {
    fn drop(&mut self) {
        // Close any cached cursors before the transaction ends. Runs
        // exactly once: this `Drop` only fires when the last `Arc<PtrSync>`
        // is released, so there is no race with surviving `TxSync` clones.
        // No-op if `commit_inner` already drained the cache.
        for cursor in self.lock.get_mut().drain_cursors() {
            // SAFETY: cursor was returned from an earlier Cursor::drop
            // bound to this still-live txn.
            unsafe { ops::cursor_close_raw(cursor) };
        }

        if self.committed.load(Ordering::SeqCst) {
            return;
        }

        if self.is_read_only {
            // RO: direct abort is safe and fast.
            // SAFETY: We have exclusive ownership of this pointer.
            unsafe { ffi::mdbx_txn_abort(self.txn) };
        } else {
            // RW: must go through txn manager for thread safety.
            let (sender, rx) = sync_channel(0);
            self.env.txn_manager().send(Abort {
                tx: RawTxPtr(self.txn),
                sender,
                span: debug_span!("txn_manager_abort"),
            });
            rx.recv().unwrap().unwrap();
        }
        tracing::debug!(target: "libmdbx", "aborted");
    }
}
