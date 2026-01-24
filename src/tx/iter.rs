use crate::{
    Cursor, MdbxError, ReadResult, TableObject, TableObjectOwned, Transaction, TransactionKind,
    error::mdbx_result,
};
use std::{borrow::Cow, marker::PhantomData, ptr};

/// Iterates over KV pairs in an MDBX database.
pub type IterKeyVals<'tx, 'cur, K, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    Iter<'tx, 'cur, K, Key, Value, { ffi::MDBX_NEXT }>;

/// An iterator over the key/value pairs in an MDBX `DUPSORT` with duplicate
/// keys, yielding the first value for each key.
///
/// See the [`Iter`] documentation for more details.
pub type IterDupKeys<'tx, 'cur, K, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    Iter<'tx, 'cur, K, Key, Value, { ffi::MDBX_NEXT_NODUP }>;

/// An iterator over the key/value pairs in an MDBX `DUPSORT`, yielding each
/// duplicate value for a specific key.
pub type IterDupVals<'tx, 'cur, K, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    Iter<'tx, 'cur, K, Key, Value, { ffi::MDBX_NEXT_DUP }>;

/// State for a iterator.
///
/// The iterator may have an initial value to yield, be active, or be at the
/// end. The initial value is set during instantiation, by cursor positioning
/// operations.
///
/// E.g. When using [`Cursor::get`] with a specific op to position the cursor,
/// the returned key/value pair can be used as the initial value for the
/// iterator.
enum IterState<'tx, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> {
    /// Initial state, may have a first value supplied.
    ///
    /// This is used when cursor positioning ops are used to set the initial
    /// position of the iterator.
    Init((Key, Value), PhantomData<&'tx ()>),
    /// Iterator is active.
    Active,
    /// Iterator has reached the end.
    End,
}

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
    'tx: 'cur,
    'cur,
    K: TransactionKind,
    Key = Cow<'tx, [u8]>,
    Value = Cow<'tx, [u8]>,
    const OP: u32 = { ffi::MDBX_NEXT },
> {
    cursor: Cow<'cur, Cursor<'tx, K>>,
    state: IterState<'tx, Key, Value>,
    _marker: PhantomData<fn() -> (Key, Value)>,
}

impl<'tx: 'cur, 'cur, K, Key, Value, const OP: u32> Iter<'tx, 'cur, K, Key, Value, OP>
where
    K: TransactionKind,
{
    /// Create a new iterator from the given cursor, starting at the given
    /// position.
    pub(crate) fn new(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        Iter { cursor, state: IterState::Active, _marker: PhantomData }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    pub(crate) fn from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new(Cow::Borrowed(cursor))
    }

    /// Create a new iterator from an owned cursor.
    pub fn from_owned(cursor: Cursor<'tx, K>) -> Self {
        Self::new(Cow::Owned(cursor))
    }

    /// Create a new iterator from the given cursor, with no items to yield.
    pub fn new_end(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        Iter { cursor, state: IterState::End, _marker: PhantomData }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    pub fn end_from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new_end(Cow::Borrowed(cursor))
    }

    /// Create a new iterator from an owned cursor.
    pub fn end_from_owned(cursor: Cursor<'tx, K>) -> Self {
        Self::new_end(Cow::Owned(cursor))
    }

    /// Create a new iterator from the given cursor, first yielding the
    /// provided key/value pair.
    pub(crate) fn new_with(cursor: Cow<'cur, Cursor<'tx, K>>, first: (Key, Value)) -> Self {
        Iter { cursor, state: IterState::Init(first, PhantomData), _marker: PhantomData }
    }

    /// Create a new iterator from a mutable reference to the given cursor,
    /// first yielding the provided key/value pair.
    pub(crate) fn from_ref_with(cursor: &'cur mut Cursor<'tx, K>, first: (Key, Value)) -> Self {
        Self::new_with(Cow::Borrowed(cursor), first)
    }

    /// Create a new iterator from an owned cursor, first yielding the
    /// provided key/value pair.
    pub fn from_owned_with(cursor: Cursor<'tx, K>, first: (Key, Value)) -> Self {
        Self::new_with(Cow::Owned(cursor), first)
    }

    /// Execute the MDBX operation.
    ///
    /// Returns `Ok(true)` if a key/value pair was found, `Ok(false)` if no more
    /// key/value pairs are available, or `Err` on error.
    fn execute_op(
        &self,
        key: &mut ffi::MDBX_val,
        data: &mut ffi::MDBX_val,
    ) -> Result<bool, MdbxError> {
        self.cursor.txn().txn_execute(|_tx| {
            let res = unsafe { ffi::mdbx_cursor_get(self.cursor.cursor(), key, data, OP) };

            match res {
                ffi::MDBX_SUCCESS => Ok(true),
                ffi::MDBX_NOTFOUND | ffi::MDBX_ENODATA | ffi::MDBX_RESULT_TRUE => Ok(false),
                _ => mdbx_result(res).map(|_| false),
            }
        })?
    }
}

impl<K, Key, Value, const OP: u32> Iter<'_, '_, K, Key, Value, OP>
where
    K: TransactionKind,
    Key: TableObjectOwned,
    Value: TableObjectOwned,
{
    /// Own the next key/value pair from the iterator.
    pub fn owned_next(&mut self) -> ReadResult<Option<(Key, Value)>>
    where
        Key: TableObjectOwned,
        Value: TableObjectOwned,
    {
        match self.state {
            IterState::Active => {}
            IterState::End => return Ok(None),
            IterState::Init(_, _) => {
                let IterState::Init(init, _) =
                    std::mem::replace(&mut self.state, IterState::Active)
                else {
                    unreachable!()
                };
                return Ok(Some(init));
            }
        }

        // If we're Active, proceed to fetch the next item.
        let mut key = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
        let mut data = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        // Modify the memory, then check the result. True if modified, false if
        // no more items.
        if !self.execute_op(&mut key, &mut data)? {
            self.state = IterState::End;
            return Ok(None);
        }

        let key = TableObject::decode_val::<K>(self.cursor.txn(), key)?;
        let data = TableObject::decode_val::<K>(self.cursor.txn(), data)?;

        Ok(Some((key, data)))
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value, const OP: u32> Iter<'tx, 'cur, K, Key, Value, OP>
where
    K: TransactionKind,
    Key: TableObject<'tx>,
    Value: TableObject<'tx>,
{
    /// Borrow the next key/value pair from the iterator.
    ///
    /// Returns `Ok(Some((key, value)))` if a key/value pair was found,
    /// `Ok(None)` if no more key/value pairs are available, or `Err` on DB
    /// access error.
    pub fn borrow_next(&mut self) -> ReadResult<Option<(Key, Value)>> {
        // Check the state first. States are ordered from most to least common.
        match self.state {
            IterState::Active => {}
            IterState::End => return Ok(None),
            IterState::Init(_, _) => {
                let IterState::Init(init, _) =
                    std::mem::replace(&mut self.state, IterState::Active)
                else {
                    unreachable!()
                };
                return Ok(Some(init));
            }
        }

        // If we're Active, proceed to fetch the next item.
        let mut key = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
        let mut data = ffi::MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        // Modify the memory, then check the result. True if modified, false if
        // no more items.
        if !self.execute_op(&mut key, &mut data)? {
            self.state = IterState::End;
            return Ok(None);
        }

        let key = TableObject::decode_val::<K>(self.cursor.txn(), key)?;
        let data = TableObject::decode_val::<K>(self.cursor.txn(), data)?;

        Ok(Some((key, data)))
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

/// An iterator over the key/value pairs in an MDBX database with duplicate
/// keys.
pub struct IterDup<
    'tx: 'cur,
    'cur,
    K: TransactionKind,
    Key = Cow<'tx, [u8]>,
    Value = Cow<'tx, [u8]>,
> {
    inner: IterDupKeys<'tx, 'cur, K, Key, Value>,
}

impl<'tx, 'cur, K, Key, Value> IterDup<'tx, 'cur, K, Key, Value>
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

impl<'tx: 'cur, 'cur, K, Key, Value> IterDup<'tx, 'cur, K, Key, Value>
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

        // SAFETY: the tx lives as long as self.inner.cursor, and the cursor op
        // we perform does not invalidate the data borrowed from the inner
        // cursor in borrow_next.
        let tx: *const Transaction<K> = self.inner.cursor.txn();
        let tx = unsafe { tx.as_ref().unwrap() };

        // The next will be the FIRST KV pair for the NEXT key in the DUPSORT
        match self.inner.borrow_next()? {
            Some((key, value)) => {
                // SAFETY: the tx is valid as per above. The FFI calls here do
                // not invalidate any data borrowed from the inner cursor.
                //
                // This is an inlined version of Cursor::new_at_position.
                let dup_cursor = tx.txn_execute(move |_| unsafe {
                    let new_cursor = ffi::mdbx_cursor_create(ptr::null_mut());
                    let res = ffi::mdbx_cursor_copy(cursor_ptr, new_cursor);
                    mdbx_result(res)?;
                    Ok::<_, MdbxError>(Cursor::new_raw(tx, new_cursor))
                })??;

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
