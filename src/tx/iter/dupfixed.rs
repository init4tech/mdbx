//! Flattening iterator for DUPFIXED tables.

use super::DupItem;
use crate::{Cursor, ReadResult, TableObject, TableObjectOwned, TransactionKind};
use std::{borrow::Cow, marker::PhantomData};

/// A flattening iterator over DUPFIXED tables.
///
/// This iterator efficiently iterates over DUPFIXED tables by fetching pages
/// of fixed-size values and yielding them individually. DUPFIXED databases
/// store duplicate values with a fixed size, allowing MDBX to pack multiple
/// values per page.
///
/// To avoid unnecessary key cloning, this iterator yields [`DupItem::NewKey`]
/// for the first value of each key, and [`DupItem::SameKey`] for subsequent
/// values of the same key.
///
/// # Type Parameters
///
/// - `'tx`: The transaction lifetime
/// - `'cur`: The cursor lifetime
/// - `K`: The transaction kind marker
/// - `Key`: The key type (must implement [`TableObject`])
/// - `Value`: The value type (must implement [`TableObjectOwned`])
///
/// # Correctness
///
/// The value size is determined at construction time from the first value
/// in the database. All values in a DUPFIXED database must have the same
/// size.
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
/// # use signet_libmdbx::{Environment, DatabaseFlags, WriteFlags, DupItem};
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
/// let mut current_key: Option<Vec<u8>> = None;
/// for result in cursor.iter_dupfixed_start::<Vec<u8>, [u8; 4]>().unwrap() {
///     match result.unwrap() {
///         DupItem::NewKey(key, value) => {
///             let num = u32::from_le_bytes(value);
///             println!("New key {:?} => {}", key, num);
///             current_key = Some(key);
///         }
///         DupItem::SameKey(value) => {
///             let num = u32::from_le_bytes(value);
///             println!("  Same key => {}", num);
///         }
///     }
/// }
/// ```
pub struct IterDupFixed<'tx, 'cur, K: TransactionKind, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>>
{
    cursor: &'cur mut Cursor<'tx, K>,
    /// The current key being iterated.
    current_key: Option<Key>,
    /// The current page of values.
    current_page: Cow<'tx, [u8]>,
    /// Current offset into the page, incremented as values are yielded.
    page_offset: usize,
    /// The fixed value size, determined at construction.
    value_size: usize,
    /// Values remaining for current key (0 = next is new key).
    remaining: usize,
    /// When true, the iterator is exhausted and will always return `None`.
    exhausted: bool,
    _marker: PhantomData<fn() -> (Key, Value)>,
}

impl<K, Key, Value> core::fmt::Debug for IterDupFixed<'_, '_, K, Key, Value>
where
    K: TransactionKind,
    Key: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let remaining_in_page = if self.value_size > 0 {
            self.current_page.len().saturating_sub(self.page_offset) / self.value_size
        } else {
            0
        };
        f.debug_struct("IterDupFixed")
            .field("exhausted", &self.exhausted)
            .field("value_size", &self.value_size)
            .field("remaining_in_page", &remaining_in_page)
            .field("remaining_for_key", &self.remaining)
            .finish()
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> IterDupFixed<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
{
    /// Returns the fixed value size (determined at construction).
    pub const fn value_size(&self) -> usize {
        self.value_size
    }

    /// Create a new, exhausted iterator.
    ///
    /// Iteration will immediately return `None`.
    pub(crate) fn new_end(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        IterDupFixed {
            cursor,
            current_key: None,
            current_page: Cow::Borrowed(&[]),
            page_offset: 0,
            value_size: 0,
            remaining: 0,
            exhausted: true,
            _marker: PhantomData,
        }
    }

    /// Create a new iterator with the given initial key, page, and value size.
    pub(crate) fn new_with(
        cursor: &'cur mut Cursor<'tx, K>,
        key: Key,
        page: Cow<'tx, [u8]>,
        value_size: usize,
    ) -> Self {
        debug_assert!(value_size > 0, "DUPFIXED value size must be greater than zero");
        // Get the count of duplicates for the current key.
        let remaining = cursor.dup_count().unwrap_or(1);
        IterDupFixed {
            cursor,
            current_key: Some(key),
            current_page: page,
            page_offset: 0,
            value_size,
            remaining,
            exhausted: false,
            _marker: PhantomData,
        }
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> IterDupFixed<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObject<'tx>,
{
    /// Consume the next value from the current page.
    ///
    /// Returns `Some(Cow<'tx, [u8]>)` containing exactly `value_size` bytes,
    /// or `None` if the page is exhausted.
    fn consume_value(&mut self) -> Option<Cow<'tx, [u8]>> {
        let end = self.page_offset.checked_add(self.value_size)?;
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
        // Try to get next page for current key
        if let Some((key, page)) = self.cursor.next_multiple::<Key, Cow<'tx, [u8]>>()? {
            self.current_key = Some(key);
            self.current_page = page;
            self.page_offset = 0;
            return Ok(true);
        }

        // No more pages for current key, move to next key
        if self.cursor.next_nodup::<Key, ()>()?.is_none() {
            self.exhausted = true;
            return Ok(false);
        }

        // Get first page for new key
        let Some(page) = self.cursor.get_multiple::<Cow<'tx, [u8]>>()? else {
            self.exhausted = true;
            return Ok(false);
        };

        // Re-fetch the key since get_multiple doesn't return it
        let Some((key, _)) = self.cursor.get_current::<Key, ()>()? else {
            self.exhausted = true;
            return Ok(false);
        };

        // New key - get dup count
        self.remaining = self.cursor.dup_count().unwrap_or(1);

        self.current_key = Some(key);
        self.current_page = page;
        self.page_offset = 0;
        Ok(true)
    }

    /// Borrow the next item from the iterator.
    ///
    /// Returns `Ok(Some(DupItem))` where the value is a `Cow<'tx, [u8]>` of
    /// exactly `value_size` bytes.
    ///
    /// Returns `Ok(None)` when the iterator is exhausted.
    pub fn borrow_next(&mut self) -> ReadResult<Option<DupItem<Key, Cow<'tx, [u8]>>>> {
        if self.exhausted {
            return Ok(None);
        }

        // Try to consume from current page
        let value = match self.consume_value() {
            Some(v) => v,
            None => {
                // Current page exhausted, fetch next page
                if !self.fetch_next_page()? {
                    return Ok(None);
                }
                self.consume_value().expect("freshly fetched page should have values")
            }
        };

        if self.remaining == 0 {
            // This is a new key (we got here via fetch_next_page which set remaining)
            self.remaining = self.remaining.saturating_sub(1);
            let key = self.current_key.take().expect("key should be set after fetch");
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        // Check if this is the first value for the current key
        // (remaining was just set and key is present)
        if self.current_key.is_some() {
            self.remaining -= 1;
            let key = self.current_key.take().expect("key should be set");
            return Ok(Some(DupItem::NewKey(key, value)));
        }

        self.remaining = self.remaining.saturating_sub(1);
        Ok(Some(DupItem::SameKey(value)))
    }

    /// Get the next item as owned data.
    ///
    /// Returns `Ok(Some(DupItem<Key, Value>))` where the value is decoded using
    /// [`TableObjectOwned::decode`].
    pub fn owned_next(&mut self) -> ReadResult<Option<DupItem<Key, Value>>>
    where
        Value: TableObjectOwned,
    {
        self.borrow_next()?
            .map(|item| match item {
                DupItem::NewKey(k, cow) => Value::decode(&cow).map(|v| DupItem::NewKey(k, v)),
                DupItem::SameKey(cow) => Value::decode(&cow).map(DupItem::SameKey),
            })
            .transpose()
    }
}

impl<'tx: 'cur, 'cur, K, Key, Value> Iterator for IterDupFixed<'tx, 'cur, K, Key, Value>
where
    K: TransactionKind,
    Key: TableObject<'tx>,
    Value: TableObjectOwned,
{
    type Item = ReadResult<DupItem<Key, Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.exhausted || self.value_size == 0 {
            return (0, Some(0));
        }
        // remaining tracks values left for current key
        (self.remaining, None)
    }
}
