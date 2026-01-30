//! Iterator types for traversing MDBX databases.
//!
//! This module provides lending iterators over key-value pairs in MDBX
//! databases. The iterators support both borrowed and owned access patterns.
//!
//! # Iterator Types
//!
//! | Iterator | Yields | Use Case |
//! |----------|--------|----------|
//! | [`Iter`] | `(Key, Value)` | Base iterator, configurable cursor op |
//! | [`IterDup`] | `(Key, Value)` | Flat iteration over DUPSORT tables |
//! | [`IterDupOfKey`] | `Value` | Single-key DUPSORT iteration |
//! | [`IterDupFixed`] | `(Key, Value)` | Flat iteration over DUPFIXED tables |
//! | [`IterDupFixedOfKey`] | `Value` | Single-key DUPFIXED iteration |
//!
//! # Borrowing vs Owning
//!
//! Iterators provide two ways to access data:
//!
//! - [`borrow_next()`](Iter::borrow_next): Returns data potentially borrowed
//!   from the database. Requires the `Key` and `Value` types to implement
//!   [`TableObject<'tx>`](crate::TableObject). This can avoid allocations
//!   when using `Cow<'tx, [u8]>`.
//!
//! - [`owned_next()`](Iter::owned_next): Returns owned data. Requires
//!   [`TableObjectOwned`](crate::TableObjectOwned). Always safe but may allocate.
//!
//! The standard [`Iterator`] trait is implemented via `owned_next()`.
//!
//! # Dirty Page Handling
//!
//! In read-write transactions, database pages may be "dirty" (modified but
//! not yet committed). The behavior of `Cow<[u8]>` depends on the
//! `return-borrowed` feature:
//!
//! - **With `return-borrowed`**: Always returns `Cow::Borrowed`, even for
//!   dirty pages. This is faster but the data may change if the transaction
//!   modifies it later.
//!
//! - **Without `return-borrowed`** (default): Dirty pages are copied to
//!   `Cow::Owned`. This is safer but allocates more.
//!
//! # Example
//!
//! ```no_run
//! # use signet_libmdbx::Environment;
//! # use std::path::Path;
//! # let env = Environment::builder().open(Path::new("/tmp/iter_example")).unwrap();
//! let txn = env.begin_ro_sync().unwrap();
//! let db = txn.open_db(None).unwrap();
//! let mut cursor = txn.cursor(db).unwrap();
//!
//! // Iterate using the standard Iterator trait (owned)
//! for result in cursor.iter_start::<Vec<u8>, Vec<u8>>().unwrap() {
//!     let (key, value) = result.expect("decode error");
//!     println!("{:?} => {:?}", key, value);
//! }
//! ```

mod base;
pub use base::Iter;

mod dup;
pub use dup::IterDup;

mod dup_key;
pub use dup_key::IterDupOfKey;

mod dupfixed;
pub use dupfixed::IterDupFixed;

mod dupfixed_key;
pub use dupfixed_key::IterDupFixedOfKey;

/// An item from a duplicate-key iterator.
///
/// This enum avoids cloning the key for every value when iterating
/// over databases with duplicate keys. The key is only provided when
/// it changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DupItem<K, V> {
    /// First value for a new key.
    NewKey(K, V),
    /// Additional value for the current key.
    SameKey(V),
}

impl<K, V> DupItem<K, V> {
    /// Returns the value, consuming self.
    pub fn into_value(self) -> V {
        match self {
            Self::NewKey(_, v) | Self::SameKey(v) => v,
        }
    }

    /// Returns a reference to the value.
    pub const fn value(&self) -> &V {
        match self {
            Self::NewKey(_, v) | Self::SameKey(v) => v,
        }
    }

    /// Returns the key if this is a new key entry.
    pub const fn key(&self) -> Option<&K> {
        match self {
            Self::NewKey(k, _) => Some(k),
            Self::SameKey(_) => None,
        }
    }

    /// Returns true if this item represents a new key.
    pub const fn is_new_key(&self) -> bool {
        matches!(self, Self::NewKey(..))
    }
}
