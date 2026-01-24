use crate::error::ReadResult;
use crate::{MdbxError, Transaction, TransactionKind};
use std::{borrow::Cow, slice};

/// A marker trait for types that can be deserialized from a database value
/// without borrowing from the transaction.
///
/// Types implementing this trait can be used with iterators that need to
/// return owned values. This is automatically implemented for any type that
/// implements [`TableObject<'a>`] for all lifetimes `'a`.
///
/// # Built-in Implementations
///
/// - [`Vec<u8>`] - Always copies data
/// - `[u8; N]` - Copies into fixed-size array - may pan
/// - `()` - Ignores data entirely
/// - [`ObjectLength`] - Returns only the length
pub trait TableObjectOwned: for<'de> TableObject<'de> {
    /// Decodes the object from the given bytes.
    ///
    /// This is the primary method to implement. Return a [`ReadError`] if
    /// the data cannot be decoded (e.g., wrong length, invalid format).
    ///
    /// If you need to borrow data directly from the database for zero-copy
    /// deserialization, also implement [`decode_borrow`](Self::decode_borrow).
    ///
    /// [`ReadError`]: crate::ReadError
    fn decode(data_val: &[u8]) -> ReadResult<Self> {
        <Self as TableObject<'_>>::decode_borrow(Cow::Borrowed(data_val))
    }
}

impl<T> TableObjectOwned for T where T: for<'de> TableObject<'de> {}

/// Decodes values read from the database into Rust types.
///
/// Implement this trait to enable reading custom types directly from MDBX.
/// The lifetime parameter `'a` allows types to borrow data from the
/// transaction when appropriate (e.g., `Cow<'a, [u8]>`).
///
/// # Implementation Guide
///
/// For most types, only implement [`decode`](Self::decode). The default
/// implementation of [`decode_val`](Self::decode_val) will call `decode`
/// with the raw ffi bytes, and is easy to misuse.
///
/// # Zero-copy Deserialization
///
/// MDBX supports zero-copy deserialization for types that can borrow data
/// directly from the database (like `Cow<'a, [u8]>`). Read-only transactions
/// ALWAYS support borrowing, while read-write transactions require checking
/// if the data is "dirty" (modified but not yet committed) first.
///
/// The `Cow<'a, [u8]>` implementation already borrows data directly from the
/// database when possible, and falls back to copying when necessary. If you
/// need similar behavior for your own types, we recommend wrapping a
/// `Cow<'a, [u8]>`.
///
/// To take advantage of zero-copy deserialization, you MUST implement
/// [`decode_borrow`](Self::decode_borrow) to handle the `Cow` case. The default
///
/// ```
/// # use std::borrow::Cow;
/// use signet_libmdbx::{TableObject, ReadResult, MdbxError};
///
/// struct MyZeroCopy<'a> (Cow<'a, [u8]>);
///
/// impl<'a> TableObject<'a> for MyZeroCopy<'a> {
///     fn decode_borrow(data: Cow<'a, [u8]>) -> ReadResult<Self> {
///        Ok(MyZeroCopy(data))
///     }
/// }
/// ```
///
/// ## Fixed-Size Types
///
/// ```
/// # use std::borrow::Cow;
/// # use signet_libmdbx::{TableObject, ReadResult, MdbxError};
/// struct Hash([u8; 32]);
///
/// impl TableObject<'_> for Hash {
///     fn decode_borrow(data: Cow<'_, [u8]>) -> ReadResult<Self> {
///         let arr: [u8; 32] = data.as_ref().try_into()
///             .map_err(|_| MdbxError::DecodeErrorLenDiff)?;
///         Ok(Self(arr))
///     }
/// }
/// ```
///
/// ## Variable-Size Types
///
/// ```
/// # use std::borrow::Cow;
/// # use signet_libmdbx::{TableObject, ReadResult, MdbxError};
/// struct VarInt(u64);
///
/// impl TableObject<'_> for VarInt {
///     fn decode_borrow(data: Cow<'_, [u8]>) -> ReadResult<Self> {
///         // Example: decode LEB128 or similar
///         let value = data.iter()
///             .take(8)
///             .enumerate()
///             .fold(0u64, |acc, (i, &b)| acc | ((b as u64) << (i * 8)));
///         Ok(Self(value))
///     }
/// }
/// ```
pub trait TableObject<'a>: Sized {
    /// Creates the object from a `Cow` of bytes. This allows for efficient
    /// handling of both owned and borrowed data.
    fn decode_borrow(data: Cow<'a, [u8]>) -> ReadResult<Self>;

    /// Decodes the value directly from the given MDBX_val pointer.
    ///
    /// **Do not implement this unless you need zero-copy borrowing.**
    ///
    /// This method is used internally to optimize deserialization for types
    /// that borrow data directly from the database (like `Cow<'a, [u8]>`).
    ///
    /// # Safety Considerations
    ///
    /// The data pointed to by `data_val` is only valid for the lifetime of
    /// the transaction. In read-write transactions, the data may be "dirty"
    /// (modified but not yet committed), requiring a copy via `mdbx_is_dirty`
    /// before borrowing.
    #[doc(hidden)]
    #[inline(always)]
    fn decode_val<K: TransactionKind>(
        tx: &'a Transaction<K>,
        data_val: ffi::MDBX_val,
    ) -> ReadResult<Self> {
        let cow = Cow::<'a, [u8]>::decode_val::<K>(tx, data_val)?;
        Self::decode_borrow(cow)
    }
}

impl<'a> TableObject<'a> for Cow<'a, [u8]> {
    fn decode_borrow(data: Cow<'a, [u8]>) -> ReadResult<Self> {
        Ok(data)
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
    fn decode_borrow(data: Cow<'_, [u8]>) -> ReadResult<Self> {
        Ok(data.into_owned())
    }
}

impl<'a> TableObject<'a> for () {
    fn decode_borrow(_: Cow<'a, [u8]>) -> ReadResult<Self> {
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
    fn decode_borrow(data: Cow<'_, [u8]>) -> ReadResult<Self> {
        Ok(Self(data.len()))
    }
}

impl<'a, const LEN: usize> TableObject<'a> for [u8; LEN] {
    fn decode_borrow(data: Cow<'a, [u8]>) -> ReadResult<Self> {
        if data.len() != LEN {
            return Err(MdbxError::DecodeErrorLenDiff.into());
        }
        let mut a = [0; LEN];
        a[..].copy_from_slice(&data);
        Ok(a)
    }
}

impl core::ops::Deref for ObjectLength {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
