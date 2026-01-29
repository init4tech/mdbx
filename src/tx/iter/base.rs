//! Base iterator implementation for MDBX cursors.

use crate::{
    Cursor, MdbxError, ReadResult, TableObject, TableObjectOwned, TransactionKind, tx::TxPtrAccess,
};
use std::{borrow::Cow, marker::PhantomData, ptr};

/// An iterator over the key/value pairs in an MDBX database.
///
/// The iteration order is determined by the `OP` const generic parameter.
/// Usually
///
/// This is a lending iterator, meaning that the key and values are borrowed
/// from the underlying cursor when possible. This allows for more efficient
/// iteration without unnecessary allocations, and can be used to create
/// deserializing iterators and other higher-level abstractions.
///
/// Whether borrowing is possible depends on the implementation of
/// [`TableObject`] for both `Key` and `Value`.
pub struct Iter<
    'tx,
    'cur,
    K: TransactionKind,
    Key = Cow<'tx, [u8]>,
    Value = Cow<'tx, [u8]>,
    const OP: u32 = { ffi::MDBX_NEXT },
> {
    pub(crate) cursor: Cow<'cur, Cursor<'tx, K>>,
    /// Pre-fetched value from cursor positioning, yielded before calling FFI.
    pending: Option<(Key, Value)>,
    /// When true, the iterator is exhausted and will always return `None`.
    exhausted: bool,
    _marker: PhantomData<fn() -> (Key, Value)>,
}

impl<K, Key, Value, const OP: u32> core::fmt::Debug for Iter<'_, '_, K, Key, Value, OP>
where
    K: TransactionKind,
    Key: core::fmt::Debug,
    Value: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter").finish()
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value, const OP: u32> Iter<'tx, 'cur, K, Key, Value, OP>
where
    K: TransactionKind,
{
    /// Create a new iterator from the given cursor, starting at the given
    /// position.
    pub(crate) fn new(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        Iter { cursor, pending: None, exhausted: false, _marker: PhantomData }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    pub(crate) fn from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new(Cow::Borrowed(cursor))
    }

    /// Create a new iterator that is already exhausted.
    ///
    /// Iteration will immediately return `None`.
    pub(crate) fn new_end(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        Iter { cursor, pending: None, exhausted: true, _marker: PhantomData }
    }

    /// Create a new, exhausted iterator from a mutable reference to the given
    /// cursor. This is usually used as a placeholder when no items are to be
    /// yielded.
    pub(crate) fn end_from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new_end(Cow::Borrowed(cursor))
    }

    /// Create a new iterator from the given cursor, first yielding the
    /// provided key/value pair.
    pub(crate) fn new_with(cursor: Cow<'cur, Cursor<'tx, K>>, first: (Key, Value)) -> Self {
        Iter { cursor, pending: Some(first), exhausted: false, _marker: PhantomData }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    /// first yielding the provided key/value pair.
    pub(crate) fn from_ref_with(cursor: &'cur mut Cursor<'tx, K>, first: (Key, Value)) -> Self {
        Self::new_with(Cow::Borrowed(cursor), first)
    }

    /// Create a new iterator from an owned cursor, first yielding the
    /// provided key/value pair.
    pub(crate) fn from_owned_with(cursor: Cursor<'tx, K>, first: (Key, Value)) -> Self {
        Self::new_with(Cow::Owned(cursor), first)
    }
}

impl<K, Key, Value, const OP: u32> Iter<'_, '_, K, Key, Value, OP>
where
    K: TransactionKind,
    Key: TableObjectOwned,
    Value: TableObjectOwned,
{
    /// Own the next key/value pair from the iterator.
    pub fn owned_next(&mut self) -> ReadResult<Option<(Key, Value)>> {
        if self.exhausted {
            return Ok(None);
        }
        if let Some(v) = self.pending.take() {
            return Ok(Some(v));
        }
        self.execute_op()
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value, const OP: u32> Iter<'tx, 'cur, K, Key, Value, OP>
where
    K: TransactionKind,
    Key: TableObject<'tx>,
    Value: TableObject<'tx>,
{
    /// Execute the MDBX operation and decode the result.
    ///
    /// Returns `Ok(Some((key, value)))` if a key/value pair was found,
    /// `Ok(None)` if no more key/value pairs are available, or `Err` on error.
    fn execute_op(&self) -> ReadResult<Option<(Key, Value)>> {
        let mut key = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
        let mut data = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        self.cursor.access().with_txn_ptr(|txn| {
            let res =
                unsafe { ffi::mdbx_cursor_get(self.cursor.cursor(), &mut key, &mut data, OP) };

            match res {
                ffi::MDBX_SUCCESS => {
                    // SAFETY: decode_val checks for dirty writes and copies if needed.
                    // The lifetime 'tx guarantees the Cow cannot outlive the transaction.
                    unsafe {
                        let key = TableObject::decode_val::<K>(txn, key)?;
                        let data = TableObject::decode_val::<K>(txn, data)?;
                        Ok(Some((key, data)))
                    }
                }
                ffi::MDBX_NOTFOUND | ffi::MDBX_ENODATA | ffi::MDBX_RESULT_TRUE => Ok(None),
                other => Err(MdbxError::from_err_code(other).into()),
            }
        })
    }

    /// Borrow the next key/value pair from the iterator.
    ///
    /// Returns `Ok(Some((key, value)))` if a key/value pair was found,
    /// `Ok(None)` if no more key/value pairs are available, or `Err` on DB
    /// access error.
    pub fn borrow_next(&mut self) -> ReadResult<Option<(Key, Value)>> {
        if self.exhausted {
            return Ok(None);
        }
        if let Some(v) = self.pending.take() {
            return Ok(Some(v));
        }
        self.execute_op()
    }
}

impl<K, Key, Value, const OP: u32> Iterator for Iter<'_, '_, K, Key, Value, OP>
where
    K: TransactionKind,
    Key: TableObjectOwned,
    Value: TableObjectOwned,
{
    type Item = ReadResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }
}
