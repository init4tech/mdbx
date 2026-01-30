//! Flat iterator for DUPSORT databases.

use super::DupItem;
use crate::{
    Cursor, MdbxError, ReadResult, TableObject, TableObjectOwned, TransactionKind, tx::TxPtrAccess,
};
use std::{marker::PhantomData, ptr};

/// A flat iterator over DUPSORT databases yielding [`DupItem`] variants.
///
/// This iterator yields every key-value pair in the database, including all
/// duplicate values. To avoid unnecessary key cloning, it yields
/// [`DupItem::NewKey`] for the first value of each key, and
/// [`DupItem::SameKey`] for subsequent values of the same key.
///
/// # Type Parameters
///
/// - `'tx`: The transaction lifetime
/// - `'cur`: The cursor lifetime
/// - `K`: The transaction kind marker
/// - `Key`: The key type (must implement [`TableObject`])
/// - `Value`: The value type (must implement [`TableObject`])
///
/// # Example
///
/// ```no_run
/// # use signet_libmdbx::{Environment, DatabaseFlags, WriteFlags, DupItem};
/// # use std::path::Path;
/// # let env = Environment::builder().open(Path::new("/tmp/dup_example")).unwrap();
/// let txn = env.begin_rw_sync().unwrap();
/// let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
///
/// // Insert duplicate values
/// txn.put(db, b"a", b"1", WriteFlags::empty()).unwrap();
/// txn.put(db, b"a", b"2", WriteFlags::empty()).unwrap();
/// txn.put(db, b"b", b"1", WriteFlags::empty()).unwrap();
/// txn.commit().unwrap();
///
/// // Iterate over all key-value pairs
/// let txn = env.begin_ro_sync().unwrap();
/// let db = txn.open_db(None).unwrap();
/// let mut cursor = txn.cursor(db).unwrap();
///
/// let mut current_key: Option<Vec<u8>> = None;
/// for result in cursor.iter_dup_start::<Vec<u8>, Vec<u8>>().unwrap() {
///     match result.unwrap() {
///         DupItem::NewKey(key, value) => {
///             println!("New key {:?} => {:?}", key, value);
///             current_key = Some(key);
///         }
///         DupItem::SameKey(value) => {
///             println!("  Same key {:?} => {:?}", current_key.as_ref().unwrap(), value);
///         }
///     }
/// }
/// ```
pub struct IterDup<
    'tx,
    'cur,
    K: TransactionKind,
    Key = std::borrow::Cow<'tx, [u8]>,
    Value = std::borrow::Cow<'tx, [u8]>,
> {
    cursor: &'cur mut Cursor<'tx, K>,
    /// Pre-fetched value from cursor positioning, yielded before calling FFI.
    pending: Option<(Key, Value)>,
    /// Values remaining for current key (0 = next is new key).
    remaining: usize,
    /// Whether we've yielded our first item yet. The first item is always NewKey.
    first_yielded: bool,
    /// When true, the iterator is exhausted and will always return `None`.
    exhausted: bool,
    _marker: PhantomData<fn() -> (Key, Value)>,
}

impl<K, Key, Value> core::fmt::Debug for IterDup<'_, '_, K, Key, Value>
where
    K: TransactionKind,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterDup")
            .field("remaining", &self.remaining)
            .field("first_yielded", &self.first_yielded)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> IterDup<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
{
    /// Create a new iterator from a mutable reference to the given cursor.
    ///
    /// The cursor must be positioned at a valid key. The iterator will start
    /// from the NEXT item after the current cursor position.
    pub(crate) fn new(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        // When continuing from an existing position, we need to get the current
        // key's dup count to know how many values remain for this key.
        // We subtract 1 because MDBX_NEXT will move to the next value.
        // If dup_count fails (cursor not positioned), we'll treat the first
        // result as a new key.
        let remaining = cursor.dup_count().ok().and_then(|c| c.checked_sub(1)).unwrap_or(0);
        IterDup {
            cursor,
            pending: None,
            remaining,
            first_yielded: false,
            exhausted: false,
            _marker: PhantomData,
        }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    /// first yielding the provided key/value pair as a new key.
    pub(crate) fn new_with(cursor: &'cur mut Cursor<'tx, K>, first: (Key, Value)) -> Self {
        // Get the count of duplicates for the current key.
        // The pending item will be the first, so remaining = count.
        let remaining = cursor.dup_count().unwrap_or(1);
        IterDup {
            cursor,
            pending: Some(first),
            remaining,
            first_yielded: false,
            exhausted: false,
            _marker: PhantomData,
        }
    }

    /// Create a new iterator that is already exhausted.
    ///
    /// Iteration will immediately return `None`.
    pub(crate) fn new_end(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        IterDup {
            cursor,
            pending: None,
            remaining: 0,
            first_yielded: true,
            exhausted: true,
            _marker: PhantomData,
        }
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> IterDup<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObject<'tx>,
    Value: TableObject<'tx>,
{
    /// Execute MDBX_NEXT and decode the result.
    fn execute_next(&self) -> ReadResult<Option<(Key, Value)>> {
        let mut key = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
        let mut data = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        self.cursor.access().with_txn_ptr(|txn| {
            let res = unsafe {
                ffi::mdbx_cursor_get(self.cursor.cursor(), &mut key, &mut data, ffi::MDBX_NEXT)
            };

            match res {
                ffi::MDBX_SUCCESS => {
                    // SAFETY: decode_val checks for dirty writes and copies if needed.
                    // The lifetime 'tx guarantees the Cow cannot outlive the transaction.
                    unsafe {
                        let key = TableObject::decode_val::<K>(txn, key)?;
                        let value = TableObject::decode_val::<K>(txn, data)?;
                        Ok(Some((key, value)))
                    }
                }
                ffi::MDBX_NOTFOUND | ffi::MDBX_ENODATA | ffi::MDBX_RESULT_TRUE => Ok(None),
                other => Err(MdbxError::from_err_code(other).into()),
            }
        })
    }

    /// Borrow the next item from the iterator.
    ///
    /// Returns `Ok(Some(DupItem))` if an item was found,
    /// `Ok(None)` if no more items are available, or `Err` on DB
    /// access error.
    pub fn borrow_next(&mut self) -> ReadResult<Option<DupItem<Key, Value>>> {
        if self.exhausted {
            return Ok(None);
        }

        // Yield pending first item (always NewKey, already counted in remaining)
        if let Some((key, value)) = self.pending.take() {
            self.first_yielded = true;
            self.remaining = self.remaining.saturating_sub(1);
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        let Some((key, value)) = self.execute_next()? else {
            self.exhausted = true;
            return Ok(None);
        };

        // First item is always NewKey (caller hasn't seen any key yet)
        if !self.first_yielded {
            self.first_yielded = true;
            self.remaining = self.remaining.saturating_sub(1);
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        if self.remaining == 0 {
            // This is a new key - get the count of duplicates
            self.remaining = self.cursor.dup_count().unwrap_or(1).saturating_sub(1);
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        self.remaining -= 1;
        Ok(Some(DupItem::SameKey(value)))
    }
}

impl<K, Key, Value> IterDup<'_, '_, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObjectOwned,
    Value: TableObjectOwned,
{
    /// Own the next item from the iterator.
    pub fn owned_next(&mut self) -> ReadResult<Option<DupItem<Key, Value>>> {
        if self.exhausted {
            return Ok(None);
        }

        // Yield pending first item (always NewKey, already counted in remaining)
        if let Some((key, value)) = self.pending.take() {
            self.first_yielded = true;
            self.remaining = self.remaining.saturating_sub(1);
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        let Some((key, value)) = self.execute_next()? else {
            self.exhausted = true;
            return Ok(None);
        };

        // First item is always NewKey (caller hasn't seen any key yet)
        if !self.first_yielded {
            self.first_yielded = true;
            self.remaining = self.remaining.saturating_sub(1);
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        if self.remaining == 0 {
            // This is a new key - get the count of duplicates
            self.remaining = self.cursor.dup_count().unwrap_or(1).saturating_sub(1);
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        self.remaining -= 1;
        Ok(Some(DupItem::SameKey(value)))
    }
}

impl<K, Key, Value> Iterator for IterDup<'_, '_, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObjectOwned,
    Value: TableObjectOwned,
{
    type Item = ReadResult<DupItem<Key, Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.exhausted {
            return (0, Some(0));
        }
        // remaining = values left for current key (excluding pending)
        // pending = pre-fetched item ready to yield
        let pending = usize::from(self.pending.is_some());
        (self.remaining + pending, None)
    }
}
