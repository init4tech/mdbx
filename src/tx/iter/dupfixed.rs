//! Flattening iterator for DUPFIXED tables.

use crate::{Cursor, ReadResult, TableObject, TransactionKind};
use std::{borrow::Cow, marker::PhantomData};

/// A flattening iterator over DUPFIXED tables.
///
/// This iterator efficiently iterates over DUPFIXED tables by fetching pages
/// of fixed-size values and yielding them individually. DUPFIXED databases
/// store duplicate values with a fixed size, allowing MDBX to pack multiple
/// values per page.
///
/// # Type Parameters
///
/// - `'tx`: The transaction lifetime
/// - `'cur`: The cursor lifetime
/// - `K`: The transaction kind marker
/// - `Key`: The key type (must implement [`TableObject`])
/// - `VALUE_SIZE`: The fixed size of each value in bytes
///
/// # Correctness
///
/// The `VALUE_SIZE` const generic **must** match the fixed value size stored
/// in the database. MDBX does not validate this at runtime. If mismatched:
///
/// - **Too large**: Values are skipped; the iterator yields fewer items than
///   exist, potentially with misaligned data.
/// - **Too small**: The iterator yields more items than exist, each containing
///   partial or corrupted data from adjacent values.
/// - **Zero**: Causes an infinite loop (caught by debug assertion).
///
/// The correct value size is determined by the size of values written to the
/// DUPFIXED database. All values under a given key must have the same size.
///
/// # Zero-Copy Operation
///
/// When possible, this iterator avoids copying data:
/// - In read-only transactions, values are borrowed directly from memory-mapped pages
/// - In read-write transactions with clean pages, values are also borrowed
/// - Only dirty pages (modified but not committed) require copying
///
/// # Example
///
/// ```no_run
/// # use signet_libmdbx::{Environment, DatabaseFlags, WriteFlags};
/// # use std::path::Path;
/// # let env = Environment::builder().open(Path::new("/tmp/dupfixed_example")).unwrap();
/// // Create a DUPFIXED database
/// let txn = env.begin_rw_sync().unwrap();
/// let db = txn.create_db(Some("my_cool_db"), DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
///
/// // Insert fixed-size values (4 bytes each)
/// txn.put(db, b"key", &1u32.to_le_bytes(), WriteFlags::empty()).unwrap();
/// txn.put(db, b"key", &2u32.to_le_bytes(), WriteFlags::empty()).unwrap();
/// txn.put(db, b"key", &3u32.to_le_bytes(), WriteFlags::empty()).unwrap();
/// txn.commit().unwrap();
///
/// // Iterate over values
/// let txn = env.begin_ro_sync().unwrap();
/// let db = txn.open_db(Some("my_cool_db")).unwrap();
/// let mut cursor = txn.cursor(db).unwrap();
///
/// for result in cursor.iter_dupfixed_start::<Vec<u8>, 4>().unwrap() {
///     let (key, value) = result.unwrap();
///     let num = u32::from_le_bytes(value);
///     println!("{:?} => {}", key, num);
/// }
/// ```
pub struct IterDupFixed<
    'tx,
    'cur,
    K: TransactionKind,
    Key = Cow<'tx, [u8]>,
    const VALUE_SIZE: usize = 0,
> {
    cursor: Cow<'cur, Cursor<'tx, K>>,
    /// The current key being iterated.
    current_key: Option<Key>,
    /// The current page of values.
    current_page: Cow<'tx, [u8]>,
    /// Current offset into the page, incremented as values are yielded.
    page_offset: usize,
    /// When true, the iterator is exhausted and will always return `None`.
    exhausted: bool,
    _marker: PhantomData<fn() -> Key>,
}

impl<K, Key, const VALUE_SIZE: usize> core::fmt::Debug for IterDupFixed<'_, '_, K, Key, VALUE_SIZE>
where
    K: TransactionKind,
    Key: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let remaining = self.current_page.len().saturating_sub(self.page_offset) / VALUE_SIZE;
        f.debug_struct("IterDupFixed")
            .field("exhausted", &self.exhausted)
            .field("remaining_in_page", &remaining)
            .finish()
    }
}

impl<'tx: 'cur, 'cur, K, Key, const VALUE_SIZE: usize> IterDupFixed<'tx, 'cur, K, Key, VALUE_SIZE>
where
    K: TransactionKind,
{
    /// Create a new, exhausted iterator.
    ///
    /// Iteration will immediately return `None`.
    pub(crate) fn new_end(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        IterDupFixed {
            cursor,
            current_key: None,
            current_page: Cow::Borrowed(&[]),
            page_offset: 0,
            exhausted: true,
            _marker: PhantomData,
        }
    }

    /// Create a new, exhausted iterator from a mutable reference to the cursor.
    pub(crate) fn end_from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new_end(Cow::Borrowed(cursor))
    }

    /// Create a new iterator with the given initial key and page.
    pub(crate) fn new_with(
        cursor: Cow<'cur, Cursor<'tx, K>>,
        key: Key,
        page: Cow<'tx, [u8]>,
    ) -> Self {
        IterDupFixed {
            cursor,
            current_key: Some(key),
            current_page: page,
            page_offset: 0,
            exhausted: false,
            _marker: PhantomData,
        }
    }

    /// Create a new iterator from a mutable reference with initial key and page.
    pub(crate) fn from_ref_with(
        cursor: &'cur mut Cursor<'tx, K>,
        key: Key,
        page: Cow<'tx, [u8]>,
    ) -> Self {
        Self::new_with(Cow::Borrowed(cursor), key, page)
    }
}

impl<'tx: 'cur, 'cur, K, Key, const VALUE_SIZE: usize> IterDupFixed<'tx, 'cur, K, Key, VALUE_SIZE>
where
    K: TransactionKind,
    Key: TableObject<'tx> + Clone,
{
    /// Consume the next value from the current page.
    ///
    /// Returns `Some(Cow<'tx, [u8]>)` containing exactly `VALUE_SIZE` bytes,
    /// or `None` if the page is exhausted.
    fn consume_value(&mut self) -> Option<Cow<'tx, [u8]>> {
        let end = self.page_offset.checked_add(VALUE_SIZE)?;
        if end > self.current_page.len() {
            return None;
        }

        let start = self.page_offset;
        self.page_offset = end;

        match &self.current_page {
            Cow::Borrowed(slice) => Some(Cow::Borrowed(&slice[start..end])),
            Cow::Owned(vec) => Some(Cow::Owned(vec[start..end].to_vec())),
        }
    }

    /// Fetch the next page of values.
    ///
    /// First tries `next_multiple` to get more pages for the current key.
    /// If that fails, moves to the next key with `next_nodup` and fetches
    /// its first page with `get_multiple`.
    ///
    /// Returns `Ok(true)` if a new page was fetched, `Ok(false)` if exhausted.
    fn fetch_next_page(&mut self) -> ReadResult<bool> {
        let cursor = self.cursor.to_mut();

        // Try to get next page for current key
        if let Some((key, page)) = cursor.next_multiple::<Key, Cow<'tx, [u8]>>()? {
            self.current_key = Some(key);
            self.current_page = page;
            self.page_offset = 0;
            return Ok(true);
        }

        // No more pages for current key, move to next key
        if cursor.next_nodup::<Key, ()>()?.is_none() {
            self.exhausted = true;
            return Ok(false);
        }

        // Get first page for new key
        let Some(page) = cursor.get_multiple::<Cow<'tx, [u8]>>()? else {
            self.exhausted = true;
            return Ok(false);
        };

        // Re-fetch the key since get_multiple doesn't return it
        let Some((key, _)) = cursor.get_current::<Key, ()>()? else {
            self.exhausted = true;
            return Ok(false);
        };

        self.current_key = Some(key);
        self.current_page = page;
        self.page_offset = 0;
        Ok(true)
    }

    /// Borrow the next key/value pair from the iterator.
    ///
    /// Returns `Ok(Some((key, value)))` where:
    /// - `key` is cloned from the current key
    /// - `value` is a `Cow<'tx, [u8]>` of exactly `VALUE_SIZE` bytes
    ///
    /// Returns `Ok(None)` when the iterator is exhausted.
    pub fn borrow_next(&mut self) -> ReadResult<Option<(Key, Cow<'tx, [u8]>)>> {
        if self.exhausted {
            return Ok(None);
        }

        // Try to consume from current page
        if let Some(value) = self.consume_value() {
            // Key is cloned for each value - cheap for Cow<[u8]>, may allocate
            // for decoded types
            let key = self.current_key.clone().expect("key should be set when page is non-empty");
            return Ok(Some((key, value)));
        }

        // Current page exhausted, fetch next page
        if !self.fetch_next_page()? {
            return Ok(None);
        }

        // Consume first value from new page
        let value = self.consume_value().expect("freshly fetched page should have values");
        let key = self.current_key.clone().expect("key should be set after fetch");
        Ok(Some((key, value)))
    }

    /// Get the next key/value pair as owned data.
    ///
    /// Returns `Ok(Some((key, [u8; VALUE_SIZE])))` where the value is copied
    /// into a fixed-size array.
    pub fn owned_next(&mut self) -> ReadResult<Option<(Key, [u8; VALUE_SIZE])>> {
        self.borrow_next().map(|opt| {
            opt.map(|(key, value)| {
                let mut arr = [0u8; VALUE_SIZE];
                arr.copy_from_slice(&value);
                (key, arr)
            })
        })
    }
}

impl<'tx: 'cur, 'cur, K, Key, const VALUE_SIZE: usize> Iterator
    for IterDupFixed<'tx, 'cur, K, Key, VALUE_SIZE>
where
    K: TransactionKind,
    Key: TableObject<'tx> + Clone,
{
    type Item = ReadResult<(Key, [u8; VALUE_SIZE])>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }
}
