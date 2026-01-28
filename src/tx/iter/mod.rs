//! Iterator types for traversing MDBX databases.
//!
//! This module provides lending iterators over key-value pairs in MDBX
//! databases. The iterators support both borrowed and owned access patterns.
//!
//! # Iterator Types
//!
//! - [`Iter`]: Base iterator with configurable cursor operation
//! - [`IterKeyVals`]: Iterates over all key-value pairs (`MDBX_NEXT`)
//! - [`IterDupKeys`]: For `DUPSORT` databases, yields first value per key
//! - [`IterDupVals`]: For `DUPSORT` databases, yields all values for one key
//! - [`IterDup`]: Nested iteration over `DUPSORT` databases
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

mod dupfixed;
pub use dupfixed::IterDupFixed;

mod dupfixed_key;
pub use dupfixed_key::IterDupFixedOfKey;
