//! Single-key flattening iterator for DUPFIXED tables.

use crate::{Cursor, ReadResult, TableObjectOwned, TransactionKind};
use std::{borrow::Cow, marker::PhantomData};

/// A single-key flattening iterator over DUPFIXED tables.
///
/// Unlike [`IterDupFixed`](super::IterDupFixed) which iterates across all keys,
/// this iterator only yields values for a single key. When all values for that
/// key are exhausted, iteration stops.
///
/// # Type Parameters
///
/// - `'tx`: The transaction lifetime
/// - `'cur`: The cursor lifetime
/// - `K`: The transaction kind marker
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
pub struct IterDupFixedOfKey<'tx, 'cur, K: TransactionKind, Value = Vec<u8>> {
    cursor: Cow<'cur, Cursor<'tx, K>>,
    /// The current page of values.
    current_page: Cow<'tx, [u8]>,
    /// Current offset into the page, incremented as values are yielded.
    page_offset: usize,
    /// The fixed value size, determined at construction.
    value_size: usize,
    /// When true, the iterator is exhausted and will always return `None`.
    exhausted: bool,
    _marker: PhantomData<fn() -> Value>,
}

impl<K, Value> core::fmt::Debug for IterDupFixedOfKey<'_, '_, K, Value>
where
    K: TransactionKind,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let remaining = if self.value_size > 0 {
            self.current_page.len().saturating_sub(self.page_offset) / self.value_size
        } else {
            0
        };
        f.debug_struct("IterDupFixedOfKey")
            .field("exhausted", &self.exhausted)
            .field("value_size", &self.value_size)
            .field("remaining_in_page", &remaining)
            .finish()
    }
}

impl<'tx: 'cur, 'cur, K, Value> IterDupFixedOfKey<'tx, 'cur, K, Value>
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
    pub(crate) fn new_end(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        IterDupFixedOfKey {
            cursor,
            current_page: Cow::Borrowed(&[]),
            page_offset: 0,
            value_size: 0,
            exhausted: true,
            _marker: PhantomData,
        }
    }

    /// Create a new, exhausted iterator from a mutable reference to the cursor.
    pub(crate) fn end_from_ref(cursor: &'cur mut Cursor<'tx, K>) -> Self {
        Self::new_end(Cow::Borrowed(cursor))
    }

    /// Create a new iterator with the given initial page and value size.
    pub(crate) fn new_with(
        cursor: Cow<'cur, Cursor<'tx, K>>,
        page: Cow<'tx, [u8]>,
        value_size: usize,
    ) -> Self {
        IterDupFixedOfKey {
            cursor,
            current_page: page,
            page_offset: 0,
            value_size,
            exhausted: false,
            _marker: PhantomData,
        }
    }

    /// Create a new iterator from a mutable reference with initial page and
    /// value size.
    pub(crate) fn from_ref_with(
        cursor: &'cur mut Cursor<'tx, K>,
        page: Cow<'tx, [u8]>,
        value_size: usize,
    ) -> Self {
        Self::new_with(Cow::Borrowed(cursor), page, value_size)
    }
}

impl<'tx: 'cur, 'cur, K, Value> IterDupFixedOfKey<'tx, 'cur, K, Value>
where
    K: TransactionKind,
{
    /// Consume the next value from the current page.
    ///
    /// Returns `Some(Cow<'tx, [u8]>)` containing exactly `value_size` bytes,
    /// or `None` if the page is exhausted.
    fn consume_value(&mut self) -> Option<Cow<'tx, [u8]>> {
        if self.value_size == 0 {
            return None;
        }

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

    /// Fetch the next page of values for the current key.
    ///
    /// Unlike
    /// [`IterDupFixed::fetch_next_page`](crate::tx::aliases::IterDupFixed),
    /// this does NOT move to the next key when pages are exhausted. It simply
    /// returns `Ok(false)` to signal exhaustion.
    ///
    /// Returns `Ok(true)` if a new page was fetched, `Ok(false)` if exhausted.
    fn fetch_next_page(&mut self) -> ReadResult<bool> {
        let cursor = self.cursor.to_mut();

        // Try to get next page for current key
        if let Some((_key, page)) = cursor.next_multiple::<(), Cow<'tx, [u8]>>()? {
            self.current_page = page;
            self.page_offset = 0;
            return Ok(true);
        }

        // No more pages for this key - done
        self.exhausted = true;
        Ok(false)
    }

    /// Borrow the next value from the iterator.
    ///
    /// Returns `Ok(Some(value))` where `value` is a `Cow<'tx, [u8]>` of exactly
    /// `value_size` bytes.
    ///
    /// Returns `Ok(None)` when the iterator is exhausted.
    pub fn borrow_next(&mut self) -> ReadResult<Option<Cow<'tx, [u8]>>> {
        if self.exhausted {
            return Ok(None);
        }

        // Try to consume from current page
        if let Some(value) = self.consume_value() {
            return Ok(Some(value));
        }

        // Current page exhausted, fetch next page
        if !self.fetch_next_page()? {
            return Ok(None);
        }

        // Consume first value from new page
        let value = self.consume_value().expect("freshly fetched page should have values");
        Ok(Some(value))
    }

    /// Get the next value as owned data.
    ///
    /// Returns `Ok(Some(Value))` where the value is decoded using
    /// [`TableObjectOwned::decode`].
    pub fn owned_next(&mut self) -> ReadResult<Option<Value>>
    where
        Value: TableObjectOwned,
    {
        self.borrow_next()?.map(|cow| Value::decode(&cow)).transpose()
    }
}

impl<'tx: 'cur, 'cur, K, Value> Iterator for IterDupFixedOfKey<'tx, 'cur, K, Value>
where
    K: TransactionKind,
    Value: TableObjectOwned,
{
    type Item = ReadResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }
}
