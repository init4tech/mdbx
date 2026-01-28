//! Iterator for DUPSORT databases with nested iteration.

use crate::{
    Cursor, MdbxError, ReadResult, TableObject, TableObjectOwned, TransactionKind,
    error::mdbx_result,
    tx::{
        TxPtrAccess,
        aliases::{IterDupKeys, IterDupVals},
        iter::Iter,
    },
};
use std::{borrow::Cow, ptr};

/// An iterator over the key/value pairs in an MDBX database with duplicate
/// keys.
pub struct IterDup<'tx, 'cur, K: TransactionKind, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> {
    inner: IterDupKeys<'tx, 'cur, K, Key, Value>,
}

impl<'tx, 'cur, K, Key, Value> core::fmt::Debug for IterDup<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
    Key: core::fmt::Debug,
    Value: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterDup").finish()
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> IterDup<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
{
    /// Create a new iterator from the given cursor, starting at the given
    /// position.
    pub(crate) fn new(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        IterDup { inner: IterDupKeys::new(cursor) }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    pub(crate) fn from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new(Cow::Borrowed(cursor))
    }

    /// Create a new iterator from an owned cursor.
    pub fn from_owned(cursor: Cursor<'tx, K>) -> Self {
        Self::new(Cow::Owned(cursor))
    }

    /// Create a new iterator from the given cursor, the inner iterator will
    /// first yield the provided key/value pair.
    pub(crate) fn new_with(cursor: Cow<'cur, Cursor<'tx, K>>, first: (Key, Value)) -> Self {
        IterDup { inner: Iter::new_with(cursor, first) }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    /// first yielding the provided key/value pair.
    pub fn from_ref_with(cursor: &'cur mut Cursor<'tx, K>, first: (Key, Value)) -> Self {
        Self::new_with(Cow::Borrowed(cursor), first)
    }

    /// Create a new iterator from the given cursor, with no items to yield.
    pub fn new_end(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        IterDup { inner: Iter::new_end(cursor) }
    }

    /// Create a new iterator from a mutable reference to the given cursor, with
    /// no items to yield.
    pub fn end_from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new_end(Cow::Borrowed(cursor))
    }

    /// Create a new iterator from an owned cursor, with no items to yield.
    pub fn end_from_owned(cursor: Cursor<'tx, K>) -> Self {
        Self::new_end(Cow::Owned(cursor))
    }
}

impl<'tx, 'cur, K, Key, Value> IterDup<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObject<'tx>,
    Value: TableObject<'tx>,
{
    /// Borrow the next key/value pair from the iterator.
    pub fn borrow_next(&mut self) -> ReadResult<Option<IterDupVals<'tx, 'cur, K, Key, Value>>> {
        // We want to use Cursor::new_at_position to create a new cursor,
        // but the kv pair may be borrowed from the inner cursor, so we need to
        // store the references first. This is just to avoid borrow checker
        // issues in the unsafe block.
        let cursor_ptr = self.inner.cursor.as_ref().cursor();

        // SAFETY: the access lives as long as self.inner.cursor, and the cursor op
        // we perform does not invalidate the data borrowed from the inner
        // cursor in borrow_next.
        let access = self.inner.cursor.access();

        // The next will be the FIRST KV pair for the NEXT key in the DUPSORT
        match self.inner.borrow_next()? {
            Some((key, value)) => {
                // SAFETY: the access is valid as per above. The FFI calls here do
                // not invalidate any data borrowed from the inner cursor.
                //
                // This is an inlined version of Cursor::new_at_position.
                let db = self.inner.cursor.as_ref().db();
                let dup_cursor = access.with_txn_ptr(move |_| unsafe {
                    let new_cursor = ffi::mdbx_cursor_create(ptr::null_mut());
                    let res = ffi::mdbx_cursor_copy(cursor_ptr, new_cursor);
                    mdbx_result(res)?;
                    Ok::<_, MdbxError>(Cursor::new_raw(access, new_cursor, db))
                })?;

                Ok(Some(IterDupVals::from_owned_with(dup_cursor, (key, value))))
            }
            None => Ok(None),
        }
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> IterDup<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObjectOwned,
    Value: TableObjectOwned,
{
    /// Own the next key/value pair from the iterator.
    pub fn owned_next(&mut self) -> ReadResult<Option<IterDupVals<'tx, 'cur, K, Key, Value>>> {
        self.borrow_next()
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> Iterator for IterDup<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObjectOwned,
    Value: TableObjectOwned,
{
    type Item = ReadResult<IterDupVals<'tx, 'cur, K, Key, Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }
}
