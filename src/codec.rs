use crate::error::ReadResult;
use crate::{MdbxError, Transaction, TransactionKind};
use std::{borrow::Cow, slice};

/// A trait for types that can be deserialized from a database value without
/// borrowing.
pub trait TableObjectOwned: for<'de> TableObject<'de> {}

impl<T> TableObjectOwned for T where T: for<'de> TableObject<'de> {}

/// Implement this to be able to decode data values
pub trait TableObject<'a>: Sized {
    /// Decodes the object from the given bytes.
    fn decode(data_val: &[u8]) -> ReadResult<Self>;

    /// Decodes the value directly from the given MDBX_val pointer.
    ///
    /// We STRONGLY recommend you avoid implementing this method. It is used
    /// internally during get operations to optimize deserialization for
    /// certain types that borrow data directly from the database (like
    /// `Cow<'a, [u8]>`).
    ///
    /// The data pointed to by `data_val` is good only for the lifetime of the
    /// transaction, so be careful when implementing this method. In addition,
    /// in the case of read-write transactions, the data may be "dirty"
    /// (modified but not yet committed), so you may need to check for that
    /// using `mdbx_is_dirty` before borrowing it.
    #[doc(hidden)]
    fn decode_val<K: TransactionKind>(
        _: &'a Transaction<K>,
        data_val: ffi::MDBX_val,
    ) -> ReadResult<Self> {
        // SAFETY: the data val is borrowed from the inner mdbx transaction,
        // so it is valid for the lifetime of the transaction.
        let s = unsafe { slice::from_raw_parts(data_val.iov_base as *const u8, data_val.iov_len) };
        Self::decode(s)
    }
}

impl<'a> TableObject<'a> for Cow<'a, [u8]> {
    fn decode(_: &[u8]) -> ReadResult<Self> {
        unreachable!()
    }

    #[doc(hidden)]
    fn decode_val<K: TransactionKind>(
        _txn: &'a Transaction<K>,
        data_val: ffi::MDBX_val,
    ) -> ReadResult<Self> {
        let s = unsafe { slice::from_raw_parts(data_val.iov_base as *const u8, data_val.iov_len) };

        #[cfg(feature = "return-borrowed")]
        {
            Ok(Cow::Borrowed(s))
        }

        #[cfg(not(feature = "return-borrowed"))]
        {
            let is_dirty = (!K::IS_READ_ONLY)
                && crate::error::mdbx_result(unsafe {
                    ffi::mdbx_is_dirty(_txn.txn(), data_val.iov_base)
                })?;

            Ok(if is_dirty { Cow::Owned(s.to_vec()) } else { Cow::Borrowed(s) })
        }
    }
}

impl TableObject<'_> for Vec<u8> {
    fn decode(data_val: &[u8]) -> ReadResult<Self> {
        Ok(data_val.to_vec())
    }
}

impl<'a> TableObject<'a> for () {
    fn decode(_: &[u8]) -> ReadResult<Self> {
        Ok(())
    }

    fn decode_val<K: TransactionKind>(_: &'a Transaction<K>, _: ffi::MDBX_val) -> ReadResult<Self> {
        Ok(())
    }
}

/// If you don't need the data itself, just its length.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectLength(pub usize);

impl TableObject<'_> for ObjectLength {
    fn decode(data_val: &[u8]) -> ReadResult<Self> {
        Ok(Self(data_val.len()))
    }
}

impl<'a, const LEN: usize> TableObject<'a> for [u8; LEN] {
    fn decode(data_val: &[u8]) -> ReadResult<Self> {
        if data_val.len() != LEN {
            return Err(MdbxError::DecodeErrorLenDiff.into());
        }
        let mut a = [0; LEN];
        a[..].copy_from_slice(data_val);
        Ok(a)
    }
}

impl core::ops::Deref for ObjectLength {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
