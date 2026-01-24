use crate::{Error, Transaction, TransactionKind};
use std::{borrow::Cow, slice};

/// Implement this to be able to decode data values
pub trait TableObject<'a>: Sized {
    /// Decodes the object from the given bytes.
    fn decode(data_val: &[u8]) -> Result<Self, Error>;

    /// Decodes the value directly from the given MDBX_val pointer.
    #[doc(hidden)]
    fn decode_val<K: TransactionKind>(
        _: &'a Transaction<K>,
        data_val: ffi::MDBX_val,
    ) -> Result<Self, Error> {
        // SAFETY: the data val is borrowed from the inner mdbx transaction,
        // so it is valid for the lifetime of the transaction.
        let s = unsafe { slice::from_raw_parts(data_val.iov_base as *const u8, data_val.iov_len) };
        Self::decode(s)
    }
}

impl<'a> TableObject<'a> for Cow<'a, [u8]> {
    fn decode(_: &[u8]) -> Result<Self, Error> {
        unreachable!()
    }

    #[doc(hidden)]
    fn decode_val<K: TransactionKind>(
        _txn: &'a Transaction<K>,
        data_val: ffi::MDBX_val,
    ) -> Result<Self, Error> {
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

impl<'a> TableObject<'a> for Vec<u8> {
    fn decode(data_val: &[u8]) -> Result<Self, Error> {
        Ok(data_val.to_vec())
    }
}

impl<'a> TableObject<'a> for () {
    fn decode(_: &[u8]) -> Result<Self, Error> {
        Ok(())
    }

    fn decode_val<K: TransactionKind>(
        _: &'a Transaction<K>,
        _: ffi::MDBX_val,
    ) -> Result<Self, Error> {
        Ok(())
    }
}

/// If you don't need the data itself, just its length.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectLength(pub usize);

impl<'a> TableObject<'a> for ObjectLength {
    fn decode(data_val: &[u8]) -> Result<Self, Error> {
        Ok(Self(data_val.len()))
    }
}

impl<'a, const LEN: usize> TableObject<'a> for [u8; LEN] {
    fn decode(data_val: &[u8]) -> Result<Self, Error> {
        if data_val.len() != LEN {
            return Err(Error::DecodeErrorLenDiff);
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
