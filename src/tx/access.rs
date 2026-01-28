use crate::{
    Environment, MdbxResult,
    sys::txn_manager::{Abort, RawTxPtr},
};
use core::fmt;
use parking_lot::{Mutex, MutexGuard};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::sync_channel,
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
#[allow(unreachable_pub)]
pub trait TxPtrAccess: fmt::Debug + sealed::Sealed {
    /// Create an instance of the implementing type from a raw transaction
    /// pointer.
    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, env: Environment) -> Self
    where
        Self: Sized;

    /// Execute a closure with the transaction pointer.
    fn with_txn_ptr<F, R>(&self, f: F) -> MdbxResult<R>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R;

    /// Execute a closure with the transaction pointer, attempting to renew
    /// the transaction if it has timed out.
    ///
    /// This is primarily used for cleanup operations (like closing cursors)
    /// that need to succeed even after a timeout. For implementations that
    /// don't support renewal (like `RoGuard` after the Arc is dropped), this
    /// falls back to `with_txn_ptr`.
    fn with_txn_ptr_for_cleanup<F, R>(&self, f: F) -> MdbxResult<R>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        // Default: just use the normal path
        self.with_txn_ptr(f)
    }

    /// Mark the transaction as committed.
    fn mark_committed(&self);

    /// Get the transaction ID by making a call into the MDBX C API.
    fn tx_id(&self) -> Option<usize> {
        let mut id = 0;
        let _ = self.with_txn_ptr(|ptr| {
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
    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, env: Environment) -> Self
    where
        Self: Sized,
    {
        T::from_ptr_and_env(ptr, env).into()
    }

    fn with_txn_ptr<F, R>(&self, f: F) -> MdbxResult<R>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        self.as_ref().with_txn_ptr(f)
    }

    fn mark_committed(&self) {
        self.as_ref().mark_committed();
    }
}

/// Wrapper for raw txn pointer for RW transactions.
pub struct PtrUnsync {
    committed: AtomicBool,
    ptr: *mut ffi::MDBX_txn,
}

impl fmt::Debug for PtrUnsync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PtrUnsync").field("committed", &self.committed).finish()
    }
}

impl TxPtrAccess for PtrUnsync {
    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, _env: Environment) -> Self
    where
        Self: Sized,
    {
        Self { committed: AtomicBool::new(false), ptr }
    }

    fn with_txn_ptr<F, R>(&self, f: F) -> MdbxResult<R>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        Ok(f(self.ptr))
    }

    fn mark_committed(&self) {
        // SAFETY:
        // Type is neither Sync nor Send, so no concurrent access is possible.
        unsafe { *self.committed.as_ptr() = true };
    }
}

impl Drop for PtrUnsync {
    fn drop(&mut self) {
        // SAFETY:
        // We have exclusive ownership of this pointer.
        unsafe {
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
/// [`TxSync`]: crate::tx::TxSync
#[derive(Debug)]
pub struct PtrSync {
    /// Raw pointer to the MDBX transaction.
    txn: *mut ffi::MDBX_txn,

    /// Whether the transaction was committed.
    committed: AtomicBool,

    /// Contains a lock to ensure exclusive access to the transaction.
    /// The inner boolean indicates the timeout status.
    lock: Mutex<bool>,

    /// The environment that owns the transaction.
    env: Environment,
}

// SAFETY: Access to the transaction is synchronized by the lock.
unsafe impl Send for PtrSync {}

// SAFETY: Access to the transaction is synchronized by the lock.
unsafe impl Sync for PtrSync {}

impl PtrSync {
    /// Acquires the inner transaction lock to guarantee exclusive access to the transaction
    /// pointer.
    pub(crate) fn lock(&self) -> MutexGuard<'_, bool> {
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

    /// Executes the given closure once the lock on the transaction is
    /// acquired. If the transaction is timed out, it will be renewed first.
    ///
    /// Returns the result of the closure or an error if the transaction renewal fails.
    #[inline]
    pub(crate) fn txn_execute_renew_on_timeout<F, T>(&self, f: F) -> MdbxResult<T>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> T,
    {
        let _lck = self.lock();

        Ok((f)(self.txn))
    }
}

impl TxPtrAccess for PtrSync {
    fn from_ptr_and_env(ptr: *mut ffi::MDBX_txn, env: Environment) -> Self
    where
        Self: Sized,
    {
        Self { committed: AtomicBool::new(false), lock: Mutex::new(false), txn: ptr, env }
    }

    fn with_txn_ptr<F, R>(&self, f: F) -> MdbxResult<R>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        let timeout_flag = self.lock();
        if *timeout_flag {
            return Err(crate::MdbxError::ReadTransactionTimeout);
        }
        let result = f(self.txn);
        Ok(result)
    }

    fn with_txn_ptr_for_cleanup<F, R>(&self, f: F) -> MdbxResult<R>
    where
        F: FnOnce(*mut ffi::MDBX_txn) -> R,
    {
        self.txn_execute_renew_on_timeout(f)
    }

    fn mark_committed(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }
}

impl Drop for PtrSync {
    fn drop(&mut self) {
        if self.committed.load(Ordering::SeqCst) {
            return;
        }

        // For simplicity, we always abort via the transaction manager.
        // RO transactions could be aborted directly, but this keeps the logic
        // uniform.
        let (sender, rx) = sync_channel(0);
        self.env.txn_manager().send(Abort {
            tx: RawTxPtr(self.txn),
            sender,
            span: debug_span!("txn_manager_abort"),
        });
        rx.recv().unwrap().unwrap();
        tracing::debug!(target: "libmdbx", "aborted");
    }
}
