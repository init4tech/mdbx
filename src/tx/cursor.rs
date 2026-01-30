use crate::{
    Database, ObjectLength, ReadError, ReadResult, TableObject, TableObjectOwned, TransactionKind,
    codec_try_optional,
    error::{MdbxResult, mdbx_result},
    flags::*,
    tx::{
        TxPtrAccess,
        aliases::IterKeyVals,
        iter::{Iter, IterDup, IterDupFixed, IterDupFixedOfKey, IterDupOfKey},
        kind::WriteMarker,
    },
};
use ffi::{
    MDBX_FIRST, MDBX_FIRST_DUP, MDBX_GET_BOTH, MDBX_GET_BOTH_RANGE, MDBX_GET_CURRENT,
    MDBX_GET_MULTIPLE, MDBX_LAST, MDBX_LAST_DUP, MDBX_NEXT, MDBX_NEXT_DUP, MDBX_NEXT_MULTIPLE,
    MDBX_NEXT_NODUP, MDBX_PREV, MDBX_PREV_DUP, MDBX_PREV_MULTIPLE, MDBX_PREV_NODUP,
    MDBX_SEEK_AND_GET_MULTIPLE, MDBX_SET, MDBX_SET_KEY, MDBX_SET_LOWERBOUND, MDBX_SET_RANGE,
    MDBX_cursor_op,
};
use std::{ffi::c_void, fmt, marker::PhantomData, ptr};

#[cfg(debug_assertions)]
use crate::tx::assertions;

/// A cursor for navigating the items within a database.
///
/// The cursor is generic over the transaction kind `K` and the access type `A`.
/// The access type determines how the cursor accesses the underlying transaction
/// pointer, allowing the same cursor implementation to work with different
/// transaction implementations.
pub struct Cursor<'tx, K>
where
    K: TransactionKind,
{
    access: &'tx K::Access,
    cursor: *mut ffi::MDBX_cursor,
    db: Database,
    _kind: PhantomData<K>,
}

impl<'tx, K> Cursor<'tx, K>
where
    K: TransactionKind,
{
    /// Creates a new cursor from a reference to a transaction access type.
    pub(crate) fn new(access: &'tx K::Access, db: Database) -> MdbxResult<Self> {
        let mut cursor: *mut ffi::MDBX_cursor = ptr::null_mut();
        access.with_txn_ptr(|txn_ptr| unsafe {
            mdbx_result(ffi::mdbx_cursor_open(txn_ptr, db.dbi(), &mut cursor))
        })?;
        Ok(Self { access, cursor, db, _kind: PhantomData })
    }

    /// Helper function for `Clone`. This should only be invoked within
    /// a `with_txn_ptr` call to ensure safety.
    fn new_at_position(other: &Self) -> MdbxResult<Self> {
        unsafe {
            let cursor = ffi::mdbx_cursor_create(ptr::null_mut());

            let res = ffi::mdbx_cursor_copy(other.cursor(), cursor);

            let s = Self { access: other.access, cursor, db: other.db, _kind: PhantomData };

            mdbx_result(res)?;

            Ok(s)
        }
    }

    /// Returns a reference to the transaction access type.
    pub(crate) const fn access(&self) -> &'tx K::Access {
        self.access
    }

    /// Returns a raw pointer to the underlying MDBX cursor.
    ///
    /// The caller **must** ensure that the pointer is not used after the
    /// lifetime of the cursor.
    pub const fn cursor(&self) -> *mut ffi::MDBX_cursor {
        self.cursor
    }

    /// Returns the database associated with this cursor.
    pub const fn db(&self) -> Database {
        self.db
    }

    /// Returns the flags of the database associated with this cursor.
    pub const fn db_flags(&self) -> DatabaseFlags {
        self.db.flags()
    }

    /// Returns `true` if the cursor is at EOF or not positioned.
    ///
    /// This can be used to check if the cursor has valid data before
    /// performing operations that depend on cursor position.
    pub fn is_eof(&self) -> bool {
        self.access.with_txn_ptr(|_| unsafe { ffi::mdbx_cursor_eof(self.cursor) })
            == ffi::MDBX_RESULT_TRUE
    }

    /// Returns the count of duplicate values for the current key.
    ///
    /// For databases without `DUP_SORT`, this always returns 1.
    /// The cursor must be positioned at a valid key.
    pub fn dup_count(&self) -> MdbxResult<usize> {
        self.access.with_txn_ptr(|_| {
            // SAFETY: cursor is valid within with_txn_ptr block
            unsafe { crate::tx::ops::cursor_dup_count(self.cursor) }
        })
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

        self.access.with_txn_ptr(|txn| {
            // SAFETY:
            // The cursor is valid as long as self is alive.
            // The transaction is also valid as long as self is alive.
            // The data in key_val and data_val is valid as long as the
            // transaction is alive, provided the page is not dirty.
            // decode_val checks for dirty pages and copies data if needed.
            unsafe {
                let v = mdbx_result(ffi::mdbx_cursor_get(
                    self.cursor,
                    &mut key_val,
                    &mut data_val,
                    op,
                ))?;
                assert_ne!(data_ptr, data_val.iov_base);
                let key_out = {
                    // MDBX wrote in new key
                    if ptr::eq(key_ptr, key_val.iov_base) {
                        None
                    } else {
                        Some(Key::decode_val::<K>(txn, key_val)?)
                    }
                };
                let data_out = Value::decode_val::<K>(txn, data_val)?;
                Ok((key_out, data_out, v))
            }
        })
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
        let (_, v, result_true) = codec_try_optional!(self.get::<(), Value>(key, data, op));
        if result_true {
            return Ok(None);
        }
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
        let (k, v, result_true) = codec_try_optional!(self.get(key, data, op));
        if result_true {
            return Ok(None);
        }
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
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());
        self.get_value(None, None, MDBX_FIRST_DUP)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at key/data pair.
    pub fn get_both<Value>(&mut self, k: &[u8], v: &[u8]) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());
        self.get_value(Some(k), Some(v), MDBX_GET_BOTH)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at given key and at first data greater than or
    /// equal to specified data.
    pub fn get_both_range<Value>(&mut self, k: &[u8], v: &[u8]) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());
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

    /// [`DatabaseFlags::DUP_FIXED`]-only: Return up to a page of duplicate data items from current
    /// cursor position. Move cursor to prepare for [`Self::next_multiple()`].
    pub fn get_multiple<Value>(&mut self) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_fixed(self.db_flags());
        self.get_value(None, None, MDBX_GET_MULTIPLE)
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Seek to the given key and return up to a page of
    /// duplicate data items. Move cursor to prepare for [`Self::next_multiple()`].
    pub fn seek_and_get_multiple<Key, Value>(
        &mut self,
        key: &[u8],
    ) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        {
            assertions::debug_assert_dup_fixed(self.db_flags());
            assertions::debug_assert_integer_key(self.db_flags(), key);
        }
        self.get_full(Some(key), None, MDBX_SEEK_AND_GET_MULTIPLE)
    }

    /// Position at last key/data item.
    pub fn last<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        self.get_full(None, None, MDBX_LAST)
    }

    /// [`DatabaseFlags::DUP_SORT`]-only: Position at last data item of current key.
    pub fn last_dup<Value>(&mut self) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());
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
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());
        self.get_full(None, None, MDBX_NEXT_DUP)
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Return up to a page of duplicate data items from next
    /// cursor position. Move cursor to prepare for `MDBX_NEXT_MULTIPLE`.
    pub fn next_multiple<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_fixed(self.db_flags());
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
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());
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
    ///
    /// For DupSort-ed databases, positions at first data item of the key.
    pub fn set<Value>(&mut self, key: &[u8]) -> ReadResult<Option<Value>>
    where
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_integer_key(self.db_flags(), key);
        self.get_value(Some(key), None, MDBX_SET)
    }

    /// Position at specified key, return both key and data.
    pub fn set_key<Key, Value>(&mut self, key: &[u8]) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_integer_key(self.db_flags(), key);
        self.get_full(Some(key), None, MDBX_SET_KEY)
    }

    /// Position at first key greater than or equal to specified key.
    pub fn set_range<Key, Value>(&mut self, key: &[u8]) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_integer_key(self.db_flags(), key);
        self.get_full(Some(key), None, MDBX_SET_RANGE)
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Position at previous page and return up to a page of
    /// duplicate data items.
    pub fn prev_multiple<Key, Value>(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_fixed(self.db_flags());
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
        #[cfg(debug_assertions)]
        assertions::debug_assert_integer_key(self.db_flags(), key);
        let (k, v, found) = codec_try_optional!(self.get(Some(key), None, MDBX_SET_LOWERBOUND));

        Ok(Some((found, k.unwrap(), v)))
    }

    /// Returns an iterator over database items.
    ///
    /// The iterator will begin with item next after the cursor, and continue
    /// until the end of the database. For new cursors, the iterator will begin
    /// with the first item in the database.
    ///
    /// If the cursor is at EOF or not positioned (e.g., after exhausting a
    /// previous iteration), it will be repositioned to the first item.
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
        if self.is_eof() {
            // Reposition to first item
            match self.first::<Key, Value>() {
                Ok(Some(first)) => return IterKeyVals::new_with(self, first),
                Ok(None) | Err(_) => return IterKeyVals::new_end(self),
            }
        }
        IterKeyVals::new(self)
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
        IterKeyVals::new(self)
    }

    /// Iterate over database items starting from the beginning of the database.
    ///
    /// For databases with duplicate data items ([`DatabaseFlags::DUP_SORT`]),
    /// the duplicate data items of each key will be returned before moving on
    /// to the next key.
    pub fn iter_start<'cur, Key, Value>(
        &'cur mut self,
    ) -> ReadResult<Iter<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        let Some(first) = self.first()? else {
            return Ok(Iter::new_end(self));
        };

        Ok(Iter::new_with(self, first))
    }

    /// Iterate over database items starting from the given key.
    ///
    /// For databases with duplicate data items ([`DatabaseFlags::DUP_SORT`]),
    /// the duplicate data items of each key will be returned before moving on
    /// to the next key.
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
            return Ok(Iter::new_end(self));
        };

        Ok(Iter::new_with(self, first))
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
    ///
    /// If the cursor is at EOF or not positioned (e.g., after exhausting a
    /// previous iteration), it will be repositioned to the first item.
    pub fn iter_dup<'cur, Key, Value>(&'cur mut self) -> IterDup<'tx, 'cur, K, Key, Value>
    where
        Key: TableObject<'tx>,
        Value: TableObject<'tx>,
    {
        if self.is_eof() {
            match self.first::<Key, Value>() {
                Ok(Some(first)) => return IterDup::new_with(self, first),
                Ok(None) | Err(_) => return IterDup::new_end(self),
            }
        }
        IterDup::<K, Key, Value>::new(self)
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
            return Ok(IterDup::new_end(self));
        };

        Ok(IterDup::new_with(self, first))
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
            return Ok(IterDup::<K, Key, Value>::new_end(self));
        };

        Ok(IterDup::new_with(self, first))
    }

    /// Iterate over the duplicates of the item in the database with the given
    /// key.
    ///
    /// This iterator yields just the values for the specified key. When all
    /// values are exhausted, iteration stops.
    pub fn iter_dup_of<'cur, Value>(
        &'cur mut self,
        key: &[u8],
    ) -> ReadResult<IterDupOfKey<'tx, 'cur, K, Value>>
    where
        'tx: 'cur,
        Value: TableObject<'tx>,
    {
        let Some(value) = self.set::<Value>(key)? else {
            return Ok(IterDupOfKey::new_end(self));
        };

        Ok(IterDupOfKey::new_with(self, value))
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Iterate over all fixed-size duplicate
    /// values starting from the beginning of the database.
    ///
    /// This iterator efficiently fetches pages of fixed-size values and yields
    /// them individually, providing a flattened view of the DUPFIXED table.
    ///
    /// The value size is determined at runtime from the first value in the
    /// database. The `Value` type parameter must implement [`TableObjectOwned`]
    /// for decoding values.
    ///
    /// Returns [`crate::MdbxError::RequiresDupFixed`] if the database does not have the
    /// [`DatabaseFlags::DUP_FIXED`] flag set.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use signet_libmdbx::{Environment, DatabaseFlags, WriteFlags, DupItem};
    /// # use std::path::Path;
    /// # let env = Environment::builder().open(Path::new("/tmp/ex")).unwrap();
    /// let txn = env.begin_ro_sync().unwrap();
    /// let db = txn.open_db(None).unwrap();
    /// let mut cursor = txn.cursor(db).unwrap();
    ///
    /// // Iterate over fixed-size values decoded as [u8; 8]
    /// for result in cursor.iter_dupfixed_start::<Vec<u8>, [u8; 8]>().unwrap() {
    ///     let value = result.unwrap().into_value();
    ///     println!("{:?}", value);
    /// }
    /// ```
    pub fn iter_dupfixed_start<'cur, Key, Value>(
        &'cur mut self,
    ) -> ReadResult<IterDupFixed<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx> + Clone,
        Value: TableObjectOwned,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_fixed(self.db_flags());

        // Position at first key and get value size via ObjectLength
        let Some((_key, ObjectLength(value_size))) = self.first::<Key, ObjectLength>()? else {
            return Ok(IterDupFixed::new_end(self));
        };

        if value_size == 0 {
            return Ok(IterDupFixed::new_end(self));
        }

        // Get first page of values for current key
        let Some(page) = self.get_multiple::<std::borrow::Cow<'tx, [u8]>>()? else {
            return Ok(IterDupFixed::new_end(self));
        };

        // Re-fetch the key since get_multiple doesn't return it
        let Some((key, _)) = self.get_current::<Key, ()>()? else {
            return Ok(IterDupFixed::new_end(self));
        };

        Ok(IterDupFixed::new_with(self, key, page, value_size))
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Iterate over all fixed-size duplicate
    /// values starting from the given key or the first key greater than it.
    ///
    /// This iterator efficiently fetches pages of fixed-size values and yields
    /// them individually, providing a flattened view of the DUPFIXED table.
    ///
    /// The value size is determined at runtime from the first value in the
    /// database. The `Value` type parameter must implement [`TableObjectOwned`]
    /// for decoding values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use signet_libmdbx::{Environment, DatabaseFlags, WriteFlags, DupItem};
    /// # use std::path::Path;
    /// # let env = Environment::builder().open(Path::new("/tmp/ex")).unwrap();
    /// let txn = env.begin_ro_sync().unwrap();
    /// let db = txn.open_db(None).unwrap();
    /// let mut cursor = txn.cursor(db).unwrap();
    ///
    /// // Iterate over fixed-size values starting from key "start"
    /// for result in cursor.iter_dupfixed_from::<Vec<u8>, [u8; 8]>(b"start").unwrap() {
    ///     let value = result.unwrap().into_value();
    ///     println!("{:?}", value);
    /// }
    /// ```
    pub fn iter_dupfixed_from<'cur, Key, Value>(
        &'cur mut self,
        key: &[u8],
    ) -> ReadResult<IterDupFixed<'tx, 'cur, K, Key, Value>>
    where
        'tx: 'cur,
        Key: TableObject<'tx> + Clone,
        Value: TableObjectOwned,
    {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_fixed(self.db_flags());

        // Position at first key >= the requested key and get value size
        let Some((found_key, ObjectLength(value_size))) =
            self.set_range::<Key, ObjectLength>(key)?
        else {
            return Ok(IterDupFixed::new_end(self));
        };

        if value_size == 0 {
            return Ok(IterDupFixed::new_end(self));
        }

        // Get first page for this key
        let Some(page) = self.get_multiple::<std::borrow::Cow<'tx, [u8]>>()? else {
            return Ok(IterDupFixed::new_end(self));
        };

        Ok(IterDupFixed::new_with(self, found_key, page, value_size))
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Iterate over all fixed-size duplicate
    /// values for a specific key.
    ///
    /// Unlike [`Self::iter_dupfixed_start`] and [`Self::iter_dupfixed_from`]
    /// which iterate across all keys, this iterator only yields values for
    /// the specified key. When all values for that key are exhausted,
    /// iteration stops.
    ///
    /// The value size is determined at runtime from the first value in the
    /// database. The `Value` type parameter must implement [`TableObjectOwned`]
    /// for decoding values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use signet_libmdbx::{Environment, DatabaseFlags, WriteFlags};
    /// # use std::path::Path;
    /// # let env = Environment::builder().open(Path::new("/tmp/ex")).unwrap();
    /// let txn = env.begin_ro_sync().unwrap();
    /// let db = txn.open_db(None).unwrap();
    /// let mut cursor = txn.cursor(db).unwrap();
    ///
    /// // Iterate over fixed-size values for a specific key
    /// for result in cursor.iter_dupfixed_of::<[u8; 8]>(b"my_key").unwrap() {
    ///     let value: [u8; 8] = result.unwrap();
    ///     println!("value: {:?}", value);
    /// }
    /// ```
    ///
    /// [`IterDupFixedOfKey`]: crate::tx::iter::IterDupFixedOfKey
    pub fn iter_dupfixed_of<'cur, Value>(
        &'cur mut self,
        key: &[u8],
    ) -> ReadResult<IterDupFixedOfKey<'tx, 'cur, K, Value>>
    where
        'tx: 'cur,
        Value: TableObjectOwned,
    {
        #[cfg(debug_assertions)]
        {
            assertions::debug_assert_dup_fixed(self.db_flags());
            assertions::debug_assert_integer_key(self.db_flags(), key);
        }

        // Position at key and get value size from the first value
        let Some(ObjectLength(value_size)) = self.set::<ObjectLength>(key)? else {
            return Ok(IterDupFixedOfKey::new_end(self));
        };

        if value_size == 0 {
            return Ok(IterDupFixedOfKey::new_end(self));
        }

        // Get first page of values (cursor is already positioned at the key)
        let Some(page) = self.get_multiple::<std::borrow::Cow<'tx, [u8]>>()? else {
            return Ok(IterDupFixedOfKey::new_end(self));
        };

        Ok(IterDupFixedOfKey::new_with(self, page, value_size))
    }
}

impl<'tx, K: TransactionKind + WriteMarker> Cursor<'tx, K> {
    /// Puts a key/data pair into the database. The cursor will be positioned at
    /// the new data item, or on failure usually near it.
    pub fn put(&mut self, key: &[u8], data: &[u8], flags: WriteFlags) -> MdbxResult<()> {
        #[cfg(debug_assertions)]
        self.access.with_txn_ptr(|txn_ptr| {
            // SAFETY: txn_ptr is valid, getting env and stat for assertion only
            let env_ptr = unsafe { ffi::mdbx_txn_env(txn_ptr) };
            let mut stat: ffi::MDBX_stat = unsafe { std::mem::zeroed() };
            unsafe {
                ffi::mdbx_env_stat_ex(
                    env_ptr,
                    std::ptr::null(),
                    &mut stat,
                    std::mem::size_of::<ffi::MDBX_stat>(),
                )
            };
            let pagesize = stat.ms_psize as usize;
            assertions::debug_assert_put(pagesize, self.db.flags(), key, data);
        });

        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let mut data_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: data.len(), iov_base: data.as_ptr() as *mut c_void };
        mdbx_result(self.access.with_txn_ptr(|_| unsafe {
            ffi::mdbx_cursor_put(self.cursor, &key_val, &mut data_val, flags.bits())
        }))
        .map(drop)
    }

    fn del_inner(&mut self, flags: WriteFlags) -> MdbxResult<()> {
        mdbx_result(
            self.access
                .with_txn_ptr(|_| unsafe { ffi::mdbx_cursor_del(self.cursor, flags.bits()) }),
        )
        .map(drop)
    }

    /// Deletes the current key/data pair.
    ///
    /// In order to delete all data items for a key in a
    /// [`DatabaseFlags::DUP_SORT`] database, see [`Cursor::del_all_dups`].
    pub fn del(&mut self) -> MdbxResult<()> {
        self.del_inner(WriteFlags::CURRENT)
    }

    /// Deletes all duplicate data items for the current key.
    ///
    /// This is a [`DatabaseFlags::DUP_SORT`]-only operation that efficiently
    /// removes all values associated with the current key in a single call.
    ///
    /// The cursor must be positioned at a valid key before calling this method.
    /// After deletion, the cursor position is undefined.
    pub fn del_all_dups(&mut self) -> MdbxResult<()> {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());
        self.del_inner(WriteFlags::ALLDUPS)
    }

    /// Delete all duplicate data items for the specified key.
    ///
    /// This is a [`DatabaseFlags::DUP_SORT`]-only operation that efficiently
    /// removes all values associated with the given key in a single call.
    ///
    /// If the key does not exist, no action is taken.
    pub fn del_all_dups_of(&mut self, key: &[u8]) -> MdbxResult<()> {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());

        // Position at the key. Convert the error to MdbxResult.
        let found = self.set::<()>(key).map_err(|e| match e {
            ReadError::Mdbx(e) => e,
            _ => unreachable!("() can always be decoded"),
        })?;

        if found.is_none() {
            // Key not found, nothing to delete
            return Ok(());
        }

        // Delete all duplicates for the current key
        self.del_inner(WriteFlags::ALLDUPS)
    }

    /// Appends a key/data pair to the end of the database.
    ///
    /// The key must be greater than all existing keys (or less than, for
    /// [`DatabaseFlags::REVERSE_KEY`] tables). This is more efficient than
    /// [`Cursor::put`] when adding data in sorted order.
    ///
    /// In debug builds, this method asserts that the key ordering constraint is
    /// satisfied.
    pub fn append(&mut self, key: &[u8], data: &[u8]) -> MdbxResult<()> {
        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let mut data_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: data.len(), iov_base: data.as_ptr() as *mut c_void };

        mdbx_result(self.access.with_txn_ptr(|_txn_ptr| {
            #[cfg(debug_assertions)]
            // SAFETY: txn_ptr is valid from with_txn_ptr.
            unsafe {
                crate::tx::ops::debug_assert_append(
                    _txn_ptr,
                    self.db.dbi(),
                    self.db.flags(),
                    key,
                    data,
                )
            };

            // SAFETY: cursor and txn_ptr are valid.
            unsafe {
                ffi::mdbx_cursor_put(
                    self.cursor,
                    &key_val,
                    &mut data_val,
                    WriteFlags::APPEND.bits(),
                )
            }
        }))
        .map(drop)
    }

    /// Appends duplicate data for [`DatabaseFlags::DUP_SORT`] databases.
    ///
    /// The data must be greater than all existing data for this key (or less
    /// than, for [`DatabaseFlags::REVERSE_DUP`] tables). This is more efficient
    /// than [`Cursor::put`] when adding duplicates in sorted order.
    ///
    /// In debug builds, this method asserts that the data ordering constraint
    /// is satisfied.
    pub fn append_dup(&mut self, key: &[u8], data: &[u8]) -> MdbxResult<()> {
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_sort(self.db_flags());

        let key_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };
        let mut data_val: ffi::MDBX_val =
            ffi::MDBX_val { iov_len: data.len(), iov_base: data.as_ptr() as *mut c_void };

        mdbx_result(self.access.with_txn_ptr(|_txn_ptr| {
            #[cfg(debug_assertions)]
            // SAFETY: _txn_ptr is valid from with_txn_ptr.
            unsafe {
                crate::tx::ops::debug_assert_append_dup(
                    _txn_ptr,
                    self.db.dbi(),
                    self.db.flags(),
                    key,
                    data,
                )
            };

            // SAFETY: cursor and txn_ptr are valid.
            unsafe {
                ffi::mdbx_cursor_put(
                    self.cursor,
                    &key_val,
                    &mut data_val,
                    WriteFlags::APPEND_DUP.bits(),
                )
            }
        }))
        .map(drop)
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Store multiple contiguous fixed-size
    /// data elements for a single key.
    ///
    /// More efficient than repeated `put()` calls when inserting many values
    /// for the same key into a DUPFIXED database.
    ///
    /// # Arguments
    /// - `key`: The key for which to store the values
    /// - `values`: Contiguous array of fixed-size values as a byte slice
    /// - `value_size`: Size of each individual value in bytes
    ///
    /// # Returns
    /// The number of values actually written. May be less than requested if
    /// duplicates already exist (with `NO_DUP_DATA` flag behavior).
    ///
    /// # Errors
    /// - [`MdbxError::RequiresDupFixed`] if database lacks `DUP_FIXED` flag
    /// - [`MdbxError::BadValSize`] if `values.len()` is not divisible by `value_size`
    /// - [`MdbxError::BadValSize`] if `value_size` is 0
    pub fn put_multiple(
        &mut self,
        key: &[u8],
        values: &[u8],
        value_size: usize,
    ) -> MdbxResult<usize> {
        self.put_multiple_inner(key, values, value_size, WriteFlags::MULTIPLE)
    }

    /// [`DatabaseFlags::DUP_FIXED`]-only: Replace all values for a key with
    /// multiple new values atomically.
    ///
    /// Combines `MDBX_MULTIPLE` with `MDBX_ALLDUPS` to atomically replace all
    /// existing duplicate values for the key.
    ///
    /// # Arguments
    /// - `key`: The key for which to replace all values
    /// - `values`: Contiguous array of fixed-size values as a byte slice
    /// - `value_size`: Size of each individual value in bytes
    ///
    /// # Returns
    /// The number of values actually written.
    ///
    /// # Errors
    /// - [`MdbxError::RequiresDupFixed`] if database lacks `DUP_FIXED` flag
    /// - [`MdbxError::BadValSize`] if `values.len()` is not divisible by `value_size`
    /// - [`MdbxError::BadValSize`] if `value_size` is 0
    pub fn put_multiple_overwrite(
        &mut self,
        key: &[u8],
        values: &[u8],
        value_size: usize,
    ) -> MdbxResult<usize> {
        self.put_multiple_inner(key, values, value_size, WriteFlags::MULTIPLE | WriteFlags::ALLDUPS)
    }

    /// Internal implementation for `put_multiple` and `put_multiple_overwrite`.
    fn put_multiple_inner(
        &mut self,
        key: &[u8],
        values: &[u8],
        value_size: usize,
        flags: WriteFlags,
    ) -> MdbxResult<usize> {
        // Validate DUP_FIXED requirement
        #[cfg(debug_assertions)]
        assertions::debug_assert_dup_fixed(self.db_flags());

        if !self.db_flags().contains(DatabaseFlags::DUP_FIXED) {
            return Err(crate::MdbxError::RequiresDupFixed);
        }

        // Validate value_size
        if value_size == 0 {
            return Err(crate::MdbxError::BadValSize);
        }

        // Validate values.len() is divisible by value_size
        if !values.len().is_multiple_of(value_size) {
            return Err(crate::MdbxError::BadValSize);
        }

        // Calculate element count; early return if nothing to insert
        let count = values.len() / value_size;
        if count == 0 {
            return Ok(0);
        }

        // Build MDBX_val structures
        let key_val = ffi::MDBX_val { iov_len: key.len(), iov_base: key.as_ptr() as *mut c_void };

        // Array of two MDBX_val as required by MDBX_MULTIPLE:
        // - data[0].iov_len = size of a single data element
        // - data[0].iov_base = pointer to contiguous array of data elements
        // - data[1].iov_len = count of elements to store (input); actual count written (output)
        // - data[1].iov_base = unused
        let mut data_vals: [ffi::MDBX_val; 2] = [
            ffi::MDBX_val { iov_len: value_size, iov_base: values.as_ptr() as *mut c_void },
            ffi::MDBX_val { iov_len: count, iov_base: ptr::null_mut() },
        ];

        // SAFETY: cursor and txn_ptr are valid within with_txn_ptr block.
        // data_vals is properly structured per MDBX_MULTIPLE requirements.
        mdbx_result(self.access.with_txn_ptr(|_| unsafe {
            ffi::mdbx_cursor_put(self.cursor, &key_val, data_vals.as_mut_ptr(), flags.bits())
        }))?;

        // Return actual count written
        Ok(data_vals[1].iov_len)
    }
}

impl<'tx, K> Clone for Cursor<'tx, K>
where
    K: TransactionKind,
{
    fn clone(&self) -> Self {
        self.access.with_txn_ptr(|_| Self::new_at_position(self).unwrap())
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
        // MDBX cursors MUST be closed. Failure to do so is a memory leak.
        //
        // To be able to close a cursor of a timed out transaction, we need to
        // renew it first. Hence the usage of `with_txn_ptr_for_cleanup` here.
        self.access.with_txn_ptr(|_| unsafe { ffi::mdbx_cursor_close(self.cursor) });
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
