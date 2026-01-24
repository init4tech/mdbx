use crate::{
    RW, ReadResult, TableObject, Transaction, TransactionKind, codec_try_optional,
    error::{MdbxResult, mdbx_result},
    flags::*,
    iter::{Iter, IterDup, IterDupVals, IterKeyVals},
};
use ffi::{
    MDBX_FIRST, MDBX_FIRST_DUP, MDBX_GET_BOTH, MDBX_GET_BOTH_RANGE, MDBX_GET_CURRENT,
    MDBX_GET_MULTIPLE, MDBX_LAST, MDBX_LAST_DUP, MDBX_NEXT, MDBX_NEXT_DUP, MDBX_NEXT_MULTIPLE,
    MDBX_NEXT_NODUP, MDBX_PREV, MDBX_PREV_DUP, MDBX_PREV_MULTIPLE, MDBX_PREV_NODUP, MDBX_SET,
    MDBX_SET_KEY, MDBX_SET_LOWERBOUND, MDBX_SET_RANGE, MDBX_cursor_op,
};
use std::{ffi::c_void, fmt, ptr};

/// A cursor for navigating the items within a database.
pub struct Cursor<'tx, K>
where
    K: TransactionKind,
{
    txn: &'tx Transaction<K>,
    cursor: *mut ffi::MDBX_cursor,
}

impl<'tx, K> Cursor<'tx, K>
where
    K: TransactionKind,
{
    pub(crate) fn new(txn: &'tx Transaction<K>, dbi: ffi::MDBX_dbi) -> MdbxResult<Self> {
        let mut cursor: *mut ffi::MDBX_cursor = ptr::null_mut();
        unsafe {
            txn.txn_execute(|txn_ptr| {
                mdbx_result(ffi::mdbx_cursor_open(txn_ptr, dbi, &mut cursor))
            })??;
        }
        Ok(Self { txn, cursor })
    }

    /// Creates a cursor from a raw MDBX cursor pointer.
    ///
    /// This function must only be used when you are certain that the provided
    pub(crate) const fn new_raw(txn: &'tx Transaction<K>, cursor: *mut ffi::MDBX_cursor) -> Self {
        Self { txn, cursor }
    }

    /// Helper function for `Clone`. This should only be invoked via
    /// [`Transaction::txn_execute`] to ensure safety.
    fn new_at_position(other: &Self) -> MdbxResult<Self> {
        unsafe {
            let cursor = ffi::mdbx_cursor_create(ptr::null_mut());

            let res = ffi::mdbx_cursor_copy(other.cursor(), cursor);

            let s = Self { txn: other.txn, cursor };

            mdbx_result(res)?;

            Ok(s)
        }
    }

    /// Returns the transaction associated with this cursor.
    pub(crate) const fn txn(&self) -> &'tx Transaction<K> {
        self.txn
    }

    /// Returns a raw pointer to the underlying MDBX cursor.
    ///
    /// The caller **must** ensure that the pointer is not used after the
    /// lifetime of the cursor.
    pub const fn cursor(&self) -> *mut ffi::MDBX_cursor {
        self.cursor
    }

    /// Retrieves a key/data pair from the cursor. Depending on the cursor op,
    /// the current key may be returned.
    fn get<Key, Value>(
        &self,
        key: Option<&[u8]>,
        data: Option<&[u8]>,
        op: MDBX_cursor_op,
    ) -> ReadResult<(Option<Key>, Value, bool)>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let mut key_val = slice_to_val(key);
        let mut data_val = slice_to_val(data);
        let key_ptr = key_val.iov_base;
        let data_ptr = data_val.iov_base;

        self.txn.txn_execute(|_txn| {
            let v = mdbx_result(unsafe {
                ffi::mdbx_cursor_get(self.cursor, &mut key_val, &mut data_val, op)
            })?;
            assert_ne!(data_ptr, data_val.iov_base);
            let key_out = {
                // MDBX wrote in new key
                if ptr::eq(key_ptr, key_val.iov_base) {
                    None
                } else {
                    Some(Key::decode_val::<K>(self.txn, key_val)?)
                }
            };
            let data_out = Value::decode_val::<K>(self.txn, data_val)?;
            Ok((key_out, data_out, v))
        })?
    }

    fn get_value<Value>(
        &mut self,
        key: Option<&[u8]>,
        data: Option<&[u8]>,
        op: MDBX_cursor_op,
    ) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        let (_, v, _) = codec_try_optional!(self.get::<(), Value>(key, data, op));

        Ok(Some(v))
    }

    fn get_full<Key, Value>(
        &mut self,
        key: Option<&[u8]>,
        data: Option<&[u8]>,
        op: MDBX_cursor_op,
    ) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let (k, v, _) = codec_try_optional!(self.get(key, data, op));

        Ok(Some((k.unwrap(), v)))
    }

    /// Position at first key/data item.
    pub fn first<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_FIRST)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at first data item of current key.
    pub fn first_dup<Value>(&mut self) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        self.get_value(None, None, MDBX_FIRST_DUP)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at key/data pair.
    pub fn get_both<Value>(&mut self, k: &[u8], v: &[u8]) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        self.get_value(Some(k), Some(v), MDBX_GET_BOTH)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at given key and at first data greater than or
    /// equal to specified data.
    pub fn get_both_range<Value>(&mut self, k: &[u8], v: &[u8]) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        self.get_value(Some(k), Some(v), MDBX_GET_BOTH_RANGE)
    }

    /// Return key/data at current cursor position.
    pub fn get_current<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_GET_CURRENT)
    }

    /// DupFixed-only: Return up to a page of duplicate data items from current cursor position.
    /// Move cursor to prepare for [`Self::next_multiple()`].
    pub fn get_multiple<Value>(&mut self) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        self.get_value(None, None, MDBX_GET_MULTIPLE)
    }

    /// Position at last key/data item.
    pub fn last<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_LAST)
    }

    /// DupSort-only: Position at last data item of current key.
    pub fn last_dup<Value>(&mut self) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        self.get_value(None, None, MDBX_LAST_DUP)
    }

    /// Position at next data item
    #[expect(clippy::should_implement_trait)]
    pub fn next<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_NEXT)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at next data item of current key.
    pub fn next_dup<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_NEXT_DUP)
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Return up to a page of duplicate data items from next
    /// cursor position. Move cursor to prepare for `MDBX_NEXT_MULTIPLE`.
    pub fn next_multiple<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_NEXT_MULTIPLE)
    }

    /// Position at first data item of next key.
    pub fn next_nodup<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_NEXT_NODUP)
    }

    /// Position at previous data item.
    pub fn prev<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_PREV)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at previous data item of current key.
    pub fn prev_dup<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_PREV_DUP)
    }

    /// Position at last data item of previous key.
    pub fn prev_nodup<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_PREV_NODUP)
    }

    /// Position at specified key.
    pub fn set<Value>(&mut self, key: &[u8]) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        self.get_value(Some(key), None, MDBX_SET)
    }

    /// Position at specified key, return both key and data.
    pub fn set_key<Key, Value>(&mut self, key: &[u8]) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(Some(key), None, MDBX_SET_KEY)
    }

    /// Position at first key greater than or equal to specified key.
    pub fn set_range<Key, Value>(&mut self, key: &[u8]) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(Some(key), None, MDBX_SET_RANGE)
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Position at previous page and return up to a page of
    /// duplicate data items.
    pub fn prev_multiple<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_PREV_MULTIPLE)
    }

    /// Position at first key-value pair greater than or equal to specified, return both key and
    /// data, and the return code depends on an exact match.
    ///
    /// For non DupSort-ed collections this works the same as [`Self::set_range()`], but returns
    /// [false] if key found exactly and [true] if greater key was found.
    ///
    /// For DupSort-ed a data value is taken into account for duplicates, i.e. for a pairs/tuples of
    /// a key and an each data value of duplicates. Returns [false] if key-value pair found
    /// exactly and [true] if the next pair was returned.
    pub fn set_lowerbound<Key, Value>(
        &mut self,
        key: &[u8],
    ) -> ReadResult<Option<(bool, Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let (k, v, found) = codec_try_optional!(self.get(Some(key), None, MDBX_SET_LOWERBOUND));

        Ok(Some((found, k.unwrap(), v)))
    }

    /// Returns an iterator over database items.
    ///
    /// The iterator will begin with item next after the cursor, and continue
    /// until the end of the database. For new cursors, the iterator will begin
    /// with the first item in the database.
    ///
    /// For databases with duplicate data items ([`DatabaseFlags::DUP_SORT`]),
    /// the duplicate data items of each key will be returned before moving on
    /// to the next key.
    pub fn iter<'cur, Key, Value>(&'cur mut self) -> IterKeyVals<'tx, 'cur, K, Key, Value>
    where
        'tx: 'cur,
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        IterKeyVals::from_ref(self)
    }

    /// Returns an iterator over database items as slices.
    ///
    /// The iterator will begin with item next after the cursor, and continue
    /// until the end of the database. For new cursors, the iterator will begin
    /// with the first item in the database.
    pub fn iter_slices<'cur>(&'cur mut self) -> IterKeyVals<'tx, 'cur, K>
    where
        'tx: 'cur,
    {
        IterKeyVals::from_ref(self)
    }

    /// Iterate over database items starting from the beginning of the database.
    ///
    /// For databases with duplicate data items ([`DatabaseFlags::DUP_SORT`]), the
    /// duplicate data items of each key will be returned before moving on to
    /// the next key.
    pub fn iter_start<'cur, Key, Value>(
        &'cur mut self,
    ) -> ReadResult<Iter<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let Some(first) = self.first()? else {
            return Ok(Iter::end_from_ref(self));
        };

        Ok(Iter::from_ref_with(self, first))
    }

    /// Iterate over database items starting from the given key.
    ///
    /// For databases with duplicate data items ([`DatabaseFlags::DUP_SORT`]), the
    /// duplicate data items of each key will be returned before moving on to
    /// the next key.
    pub fn iter_from<'cur, Key, Value>(
        &'cur mut self,
        key: &[u8],
    ) -> ReadResult<Iter<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let Some(first) = self.set_range::<Key, Value>(key)? else {
            return Ok(Iter::end_from_ref(self));
        };

        Ok(Iter::from_ref_with(self, first))
    }

    /// Iterate over duplicate database items.
    ///
    /// The iterator will produce an iterator for each key in the database,
    /// yielding all duplicate data items for that key.
    ///
    /// Like [`Self::iter`], this function will start with the key AFTER the
    /// current cursor position, and continue until the end of the database.
    /// For new cursors, the iterator will begin with the first key in the
    /// database.
    pub fn iter_dup<'cur, Key, Value>(&'cur mut self) -> IterDup<'tx, 'cur, K, Key, Value>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        IterDup::from_ref(self)
    }

    /// Iterate over duplicate database items starting from the beginning of the
    /// database. Each item will be returned as an iterator of its duplicates.
    pub fn iter_dup_start<'cur, Key, Value>(
        &'cur mut self,
    ) -> ReadResult<IterDup<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let Some(first) = self.first()? else {
            return Ok(IterDup::end_from_ref(self));
        };

        Ok(IterDup::from_ref_with(self, first))
    }

    /// Iterate over duplicate items in the database starting from the given
    /// key. Each item will be returned as an iterator of its duplicates.
    pub fn iter_dup_from<'cur, Key, Value>(
        &'cur mut self,
        key: &[u8],
    ) -> ReadResult<IterDup<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let Some(first) = self.set_range(key)? else {
            return Ok(IterDup::end_from_ref(self));
        };

        Ok(IterDup::from_ref_with(self, first))
    }

    /// Iterate over the duplicates of the item in the database with the given
    /// key.
    pub fn iter_dup_of<'cur, Key, Value>(
        &'cur mut self,
        key: &[u8],
    ) -> ReadResult<IterDupVals<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx> + PartialEq,
        Value: TableObject<'tx>,
    {
        let Some(first) = self.set_key(key.as_ref())? else {
            return Ok(IterDupVals::end_from_ref(self));
        };

        Ok(IterDupVals::from_ref_with(self, first))
    }
}

impl<'tx> Cursor<'tx, RW> {
    /// Puts a key/data pair into the database. The cursor will be positioned at
    /// the new data item, or on failure usually near it.
    pub fn put(&mut self, key: &[u8], data: &[u8], flags: WriteFlags) -> MdbxResult<()> {
        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let mut data_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: data.len(), iov_base: data.as_ptr() as *mut c_void };
        mdbx_result(unsafe {
            self.txn.txn_execute(|_| {
                ffi::mdbx_cursor_put(self.cursor, &key_val, &mut data_val, flags.bits())
            })?
        })?;

        Ok(())
    }

    /// Deletes the current key/data pair.
    ///
    /// ### Flags
    ///
    /// [`WriteFlags::NO_DUP_DATA`] may be used to delete all data items for the
    /// current key, if the database was opened with [`DatabaseFlags::DUP_SORT`].
    pub fn del(&mut self, flags: WriteFlags) -> MdbxResult<()> {
        mdbx_result(unsafe {
            self.txn.txn_execute(|_| ffi::mdbx_cursor_del(self.cursor, flags.bits()))?
        })?;

        Ok(())
    }
}

impl<'tx, K> Clone for Cursor<'tx, K>
where
    K: TransactionKind,
{
    fn clone(&self) -> Self {
        self.txn.txn_execute(|_| Self::new_at_position(self).unwrap()).unwrap()
    }
}

impl<'tx, K> fmt::Debug for Cursor<'tx, K>
where
    K: TransactionKind,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cursor").finish_non_exhaustive()
    }
}

impl<'tx, K> Drop for Cursor<'tx, K>
where
    K: TransactionKind,
{
    fn drop(&mut self) {
        // To be able to close a cursor of a timed out transaction, we need to renew it first.
        // Hence the usage of `txn_execute_renew_on_timeout` here.
        let _ = self
            .txn
            .txn_execute_renew_on_timeout(|_| unsafe { ffi::mdbx_cursor_close(self.cursor) });
    }
}

const fn slice_to_val(slice: Option<&[u8]>) -> ffi::MDBX_val {
    match slice {
        Some(slice) => {
            ffi::MDBX_val { iov_len: slice.len(), iov_base: slice.as_ptr() as *mut c_void }
        }
        None => ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() },
    }
}

unsafe impl<'tx, K> Send for Cursor<'tx, K> where K: TransactionKind {}
unsafe impl<'tx, K> Sync for Cursor<'tx, K> where K: TransactionKind {}
