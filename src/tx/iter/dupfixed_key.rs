//! Single-key flattening iterator for DUPFIXED tables.

use crate::{Cursor, ReadResult, TransactionKind};
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
pub struct IterDupFixedOfKey<'tx, 'cur, K: TransactionKind, const VALUE_SIZE: usize = 0> {
    cursor: Cow<'cur, Cursor<'tx, K>>,
    /// The current page of values.
    current_page: Cow<'tx, [u8]>,
    /// Current offset into the page, incremented as values are yielded.
    page_offset: usize,
    /// When true, the iterator is exhausted and will always return `None`.
    exhausted: bool,
    _marker: PhantomData<fn() -> ()>,
}

impl<K, const VALUE_SIZE: usize> core::fmt::Debug for IterDupFixedOfKey<'_, '_, K, VALUE_SIZE>
where
    K: TransactionKind,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let remaining = self.current_page.len().saturating_sub(self.page_offset) / VALUE_SIZE;
        f.debug_struct("IterDupFixedOfKey")
            .field("exhausted", &self.exhausted)
            .field("remaining_in_page", &remaining)
            .finish()
    }
}

impl<'tx: 'cur, 'cur, K, const VALUE_SIZE: usize> IterDupFixedOfKey<'tx, 'cur, K, VALUE_SIZE>
where
    K: TransactionKind,
{
    /// Create a new, exhausted iterator.
    ///
    /// Iteration will immediately return `None`.
    pub(crate) fn new_end(cursor: Cow<'cur, Cursor<'tx, K>>) -> Self {
        IterDupFixedOfKey {
            cursor,
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

    /// Create a new iterator with the given initial page.
    pub(crate) fn new_with(cursor: Cow<'cur, Cursor<'tx, K>>, page: Cow<'tx, [u8]>) -> Self {
        IterDupFixedOfKey {
            cursor,
            current_page: page,
            page_offset: 0,
            exhausted: false,
            _marker: PhantomData,
        }
    }

    /// Create a new iterator from a mutable reference with initial page.
    pub(crate) fn from_ref_with(cursor: &'cur mut Cursor<'tx, K>, page: Cow<'tx, [u8]>) -> Self {
        Self::new_with(Cow::Borrowed(cursor), page)
    }
}

impl<'tx: 'cur, 'cur, K, const VALUE_SIZE: usize> IterDupFixedOfKey<'tx, 'cur, K, VALUE_SIZE>
where
    K: TransactionKind,
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
    /// `VALUE_SIZE` bytes.
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
    /// Returns `Ok(Some([u8; VALUE_SIZE]))` where the value is copied
    /// into a fixed-size array.
    pub fn owned_next(&mut self) -> ReadResult<Option<[u8; VALUE_SIZE]>> {
        self.borrow_next().map(|opt| {
            opt.map(|value| {
                let mut arr = [0u8; VALUE_SIZE];
                arr.copy_from_slice(&value);
                arr
            })
        })
    }
}

impl<'tx: 'cur, 'cur, K, const VALUE_SIZE: usize> Iterator
    for IterDupFixedOfKey<'tx, 'cur, K, VALUE_SIZE>
where
    K: TransactionKind,
{
    type Item = ReadResult<[u8; VALUE_SIZE]>;

    fn next(&mut self) -> Option<Self::Item> {
        self.owned_next().transpose()
    }
}
