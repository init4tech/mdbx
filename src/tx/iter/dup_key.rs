//! Single-key iterator for DUPSORT databases.

use crate::{
    Cursor, MdbxError, ReadResult, TableObject, TableObjectOwned, TransactionKind, tx::TxPtrAccess,
};
use std::{marker::PhantomData, ptr};

/// A single-key iterator for DUPSORT databases, yielding just values.
///
/// Unlike [`IterDup`](super::IterDup) which iterates across all keys yielding
/// `(Key, Value)` pairs, this iterator only yields values for a single key.
/// When all values for that key are exhausted, iteration stops.
///
/// # Type Parameters
///
/// - `'tx`: The transaction lifetime
/// - `'cur`: The cursor lifetime
/// - `K`: The transaction kind marker
/// - `Value`: The value type (must implement [`TableObject`])
///
/// # Example
///
/// ```no_run
/// # use signet_libmdbx::{Environment, DatabaseFlags, WriteFlags};
/// # use std::path::Path;
/// # let env = Environment::builder().open(Path::new("/tmp/dup_key_example")).unwrap();
/// let txn = env.begin_rw_sync().unwrap();
/// let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
///
/// // Insert duplicate values for a key
/// txn.put(db, b"key", b"val1", WriteFlags::empty()).unwrap();
/// txn.put(db, b"key", b"val2", WriteFlags::empty()).unwrap();
/// txn.put(db, b"key", b"val3", WriteFlags::empty()).unwrap();
/// txn.commit().unwrap();
///
/// // Iterate over values for a specific key
/// let txn = env.begin_ro_sync().unwrap();
/// let db = txn.open_db(None).unwrap();
/// let mut cursor = txn.cursor(db).unwrap();
///
/// for result in cursor.iter_dup_of::<Vec<u8>>(b"key").unwrap() {
///     let value = result.unwrap();
///     println!("value: {:?}", value);
/// }
/// ```
pub struct IterDupOfKey<'tx, 'cur, K: TransactionKind, Value = std::borrow::Cow<'tx, [u8]>> {
    cursor: &'cur mut Cursor<'tx, K>,
    /// Pre-fetched value from cursor positioning, yielded before calling FFI.
    pending: Option<Value>,
    /// When true, the iterator is exhausted and will always return `None`.
    exhausted: bool,
    _marker: PhantomData<fn() -> Value>,
}

impl<K, Value> core::fmt::Debug for IterDupOfKey<'_, '_, K, Value>
where
    K: TransactionKind,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IterDupOfKey").field("exhausted", &self.exhausted).finish()
    }
}

impl<'tx: 'cur, 'cur, K, Value> IterDupOfKey<'tx, 'cur, K, Value>
where
    K: TransactionKind,
{
    /// Create a new iterator that is already exhausted.
    ///
    /// Iteration will immediately return `None`.
    pub(crate) fn new_end(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        IterDupOfKey { cursor, pending: None, exhausted: true, _marker: PhantomData }
    }

    /// Create a new iterator with the provided first value.
    pub(crate) fn new_with(cursor: &'cur mut Cursor<'tx, K>, first: Value) -> Self {
        IterDupOfKey { cursor, pending: Some(first), exhausted: false, _marker: PhantomData }
    }
}

impl<'tx: 'cur, 'cur, K, Value> IterDupOfKey<'tx, 'cur, K, Value>
where
    K: TransactionKind,
    Value: TableObject<'tx>,
{
    /// Execute MDBX_NEXT_DUP and decode the value.
    fn execute_next_dup(&self) -> ReadResult<Option<Value>> {
        let mut key = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
        let mut data = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        self.cursor.access().with_txn_ptr(|txn| {
            let res = unsafe {
                ffi::mdbx_cursor_get(self.cursor.cursor(), &mut key, &mut data, ffi::MDBX_NEXT_DUP)
            };

            match res {
                ffi::MDBX_SUCCESS => {
                    // SAFETY: decode_val checks for dirty writes and copies if needed.
                    // The lifetime 'tx guarantees the Cow cannot outlive the transaction.
                    unsafe {
                        let value = TableObject::decode_val::<K>(txn, data)?;
                        Ok(Some(value))
                    }
                }
                ffi::MDBX_NOTFOUND | ffi::MDBX_ENODATA | ffi::MDBX_RESULT_TRUE => Ok(None),
                other => Err(MdbxError::from_err_code(other).into()),
            }
        })
    }

    /// Borrow the next value from the iterator.
    ///
    /// Returns `Ok(Some(value))` if a value was found,
    /// `Ok(None)` if no more values are available for this key, or `Err` on DB
    /// access error.
    pub fn borrow_next(&mut self) -> ReadResult<Option<Value>> {
        if self.exhausted {
            return Ok(None);
        }
        if let Some(v) = self.pending.take() {
            return Ok(Some(v));
        }
        let result = self.execute_next_dup()?;
        if result.is_none() {
            self.exhausted = true;
        }
        Ok(result)
    }
}

impl<K, Value> IterDupOfKey<'_, '_, K, Value>
where
    K: TransactionKind,
    Value: TableObjectOwned,
{
    /// Own the next value from the iterator.
    pub fn owned_next(&mut self) -> ReadResult<Option<Value>> {
        if self.exhausted {
            return Ok(None);
        }
        if let Some(v) = self.pending.take() {
            return Ok(Some(v));
        }
        let result = self.execute_next_dup()?;
        if result.is_none() {
            self.exhausted = true;
        }
        Ok(result)
    }
}

impl<K, Value> Iterator for IterDupOfKey<'_, '_, K, Value>
where
    K: TransactionKind,
    Value: TableObjectOwned,
{
    type Item = ReadResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }
}
