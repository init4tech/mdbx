//! A view of data borrowed from a transaction.
//!
//! This module provides [`TxView`], a wrapper that ensures transaction validity
//! is checked before accessing borrowed data.

use crate::{
    MdbxError, MdbxResult, RW, ReadResult, TableObjectOwned,
    tx::{PtrSyncInner, RwUnsync, TxPtrAccess},
};
use std::borrow::Cow;

/// A view of data borrowed from a transaction.
///
/// This wrapper ensures transaction validity is checked before accessing
/// the underlying data. For RW transactions and RO transactions without
/// the `read-tx-timeouts` feature, validity checks compile to no-ops.
///
/// # Safety Rationale
///
/// When the `read-tx-timeouts` feature is enabled, RO transactions can be
/// aborted asynchronously by a timeout thread. Data borrowed from the
/// transaction (like `Cow::Borrowed` slices) can become dangling if the
/// transaction times out while the borrowed data is still in use.
///
/// `TxView` addresses this by:
/// 1. Holding a reference to the transaction's access type
/// 2. Checking transaction validity before returning the data
/// 3. Compiling to zero overhead when no runtime check is needed
///
/// # Example
///
/// ```ignore
/// let view = txn.get(db.dbi(), b"key")?;
/// if let Some(view) = view {
///     let data = view.try_get()?;
///     // Use data...
/// }
/// ```
pub struct TxView<'tx, A, T = Cow<'tx, [u8]>> {
    data: T,
    access: &'tx A,
}

impl<'tx, A, T> TxView<'tx, A, T> {
    /// Creates a new `TxView`.
    #[inline]
    pub(crate) const fn new(data: T, access: &'tx A) -> Self {
        Self { data, access }
    }
}

impl<'tx, A, T> TxView<'tx, A, T>
where
    A: TxPtrAccess,
    T: TableObjectOwned,
{
    /// Access the data by value.
    pub fn into_owned(self) -> T {
        self.data
    }
}

impl<'tx, A, T> TxView<'tx, A, T>
where
    A: TxPtrAccess,
{
    /// Checks if data view is still valid.
    ///
    /// Returns `true` if the underlying transaction is still valid or if no
    /// runtime validity check is needed (e.g., RW transactions cannot time
    /// out).
    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        !A::HAS_RUNTIME_CHECK || self.access.valid()
    }

    /// Enforce that the transaction is still valid.
    #[inline(always)]
    pub fn enforce_valid(&self) -> MdbxResult<()> {
        if A::HAS_RUNTIME_CHECK && !self.access.valid() {
            return Err(MdbxError::ReadTransactionTimeout);
        }
        Ok(())
    }

    /// Access the data after checking transaction validity.
    ///
    /// Returns `Err(MdbxError::ReadTransactionTimeout)` if the transaction
    /// has timed out.
    ///
    /// For RW transactions and RO transactions without the `read-tx-timeouts`
    /// feature, this check compiles to a no-op.
    #[inline]
    pub fn try_get(&self) -> MdbxResult<&T> {
        self.enforce_valid()?;
        Ok(&self.data)
    }

    /// Access the data after checking transaction validity.
    #[inline]
    pub fn inspect<F>(&self, f: F) -> ReadResult<()>
    where
        F: FnOnce(&T),
    {
        self.enforce_valid()?;
        f(&self.data);
        Ok(())
    }

    /// Map the inner data to another type while preserving transaction access.
    ///
    /// This is useful for transforming the data while still ensuring
    /// transaction validity checks are in place.
    #[inline]
    pub fn map<U, F>(self, f: F) -> ReadResult<TxView<'tx, A, U>>
    where
        F: FnOnce(T) -> U,
    {
        self.enforce_valid()?;
        Ok(TxView::new(f(self.data), self.access))
    }

    /// Map the inner data to another type that may fail, while preserving
    /// transaction access.
    #[inline]
    pub fn flat_map<U, F>(self, f: F) -> ReadResult<TxView<'tx, A, U>>
    where
        F: FnOnce(T) -> ReadResult<U>,
    {
        if A::HAS_RUNTIME_CHECK && !self.access.valid() {
            return Err(MdbxError::ReadTransactionTimeout.into());
        }
        Ok(TxView::new(f(self.data)?, self.access))
    }

    /// Access the data without validity check.
    ///
    /// # Safety
    ///
    /// The caller must ensure the transaction is still valid. Using the
    /// returned reference after the transaction has been aborted or timed
    /// out is undefined behavior.
    #[inline]
    pub const unsafe fn get_unchecked(&self) -> &T {
        &self.data
    }

    /// Consume the view and take ownership of the inner data.
    ///
    /// This is useful when you need to outlive the transaction or want to
    /// avoid repeated validity checks.
    ///
    /// # Safety
    ///
    /// The caller must ensure the transaction is still valid. If the data
    /// borrows from the transaction (e.g., `Cow::Borrowed` slices), using
    /// the returned data after the transaction has been aborted or timed
    /// out is undefined behavior.
    #[inline]
    pub unsafe fn into_inner(self) -> T {
        self.data
    }
}

impl<'tx, A, T> TxView<'tx, A, T>
where
    T: AsRef<[u8]>,
    A: TxPtrAccess,
{
    /// Returns the length of the data.
    pub fn try_len(&self) -> MdbxResult<usize> {
        self.enforce_valid()?;
        Ok(self.data.as_ref().len())
    }
}

impl<'tx, A, T> Copy for TxView<'tx, A, T>
where
    A: TxPtrAccess,
    T: TableObjectOwned + Copy,
{
}

impl<'tx, A, T> Clone for TxView<'tx, A, T>
where
    A: TxPtrAccess,
    T: Clone + TableObjectOwned,
{
    fn clone(&self) -> Self {
        Self { data: self.data.clone(), access: self.access }
    }
}

impl<'tx, A, T> TxView<'tx, A, T>
where
    A: TxPtrAccess,
    T: Clone,
{
    /// Clone the inner data after checking transaction validity.
    ///
    /// Returns `Err(MdbxError::ReadTransactionTimeout)` if the transaction
    /// has timed out.
    #[inline]
    pub fn try_clone_inner(&self) -> MdbxResult<T> {
        self.enforce_valid()?;
        Ok(self.data.clone())
    }
}

impl<'tx, A, T> core::fmt::Debug for TxView<'tx, A, T>
where
    A: TxPtrAccess,
    T: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Check validity before displaying data to avoid showing stale data
        if A::HAS_RUNTIME_CHECK && !self.access.valid() {
            f.debug_struct("TxView").field("data", &"<timed out>").finish()
        } else {
            f.debug_struct("TxView").field("data", &self.data).finish()
        }
    }
}

// TxView is Send if both T is Send and A is Sync
unsafe impl<'tx, A, T> Send for TxView<'tx, A, T>
where
    A: TxPtrAccess + Sync,
    T: Send,
{
}

// TxView is Sync if both T is Sync and A is Sync
unsafe impl<'tx, A, T> Sync for TxView<'tx, A, T>
where
    A: TxPtrAccess + Sync,
    T: Sync,
{
}

macro_rules! impl_direct_access {
    ($ty:ty) => {

        impl<'tx, T> TxView<'tx, $ty, T>
        {
            /// Access the data without validity check.
            // Safe because RW transactions cannot time out.
            #[inline]
            pub const fn get(&self) -> &T {
                &self.data
            }
        }

        impl<T> AsRef<T> for TxView<'_, $ty, T> {
            fn as_ref(&self) -> &T {
                &self.data
            }
        }

        impl<T> std::ops::Deref for TxView<'_, $ty, T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.data
            }
        }

    };
    ($($ty:ty),+) => {
        $(impl_direct_access!($ty);)+
    };
}

impl_direct_access!(RwUnsync, PtrSyncInner<RW>);

// When read-tx-timeouts feature is disabled, RO transactions cannot time out.
#[cfg(not(feature = "read-tx-timeouts"))]
impl_direct_access!(crate::tx::RoGuard, crate::tx::PtrSyncInner<crate::tx::RO>);
