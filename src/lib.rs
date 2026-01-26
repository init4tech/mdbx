//! Idiomatic and safe Rust bindings for [libmdbx].
//!
//! # Overview
//!
//! [libmdbx] is a high-performance embedded key-value database based on
//! LMDB, with additional features like nested transactions, automatic
//! compaction, and improved durability options.
//!
//! This crate provides a safe, idiomatic Rust interface for:
//! - Creating and managing memory-mapped database environments
//! - Performing transactional read and write operations
//! - Iterating over key-value pairs with cursors
//! - Custom serialization via the [`TableObject`] trait
//!
//! # Quick Start
//!
//! Databases are stored in a directory on disk. The following example
//! demonstrates creating an environment, writing a key-value pair, and
//! reading it back.
//!
//! ```no_run
//! use signet_libmdbx::{
//!     Environment, DatabaseFlags, WriteFlags, Geometry, MdbxResult,
//! };
//! use std::path::Path;
//!
//! fn main() -> MdbxResult<()> {
//!     // Open an environment (creates directory if needed)
//!     let env = Environment::builder()
//!         .set_geometry(Geometry {
//!             size: Some(0..(1024 * 1024 * 1024)), // up to 1GB
//!             ..Default::default()
//!         })
//!         .open(Path::new("/tmp/my_database"))?;
//!
//!     // Write data in a read-write transaction
//!     let txn = env.begin_rw_txn()?;
//!     let db = txn.create_db(None, DatabaseFlags::empty())?;
//!     txn.put(db.dbi(), b"hello", b"world", WriteFlags::empty())?;
//!     txn.commit()?;
//!
//!     // Read data in a read-only transaction
//!     let txn = env.begin_ro_txn()?;
//!     let db = txn.open_db(None)?;
//!     let value: Option<Vec<u8>> = txn.get(db.dbi(), b"hello").expect("read failed");
//!     assert_eq!(value.as_deref(), Some(b"world".as_slice()));
//!
//!     Ok(())
//! }
//! ```
//!
//! # Key Concepts
//!
//! - **Environment**: A directory containing one or more databases. Created
//!   via [`Environment::builder()`].
//! - **Transaction**: All operations occur within transactions. Use
//!   [`Environment::begin_ro_txn()`] for reads and
//!   [`Environment::begin_rw_txn()`] for read-writes.
//! - [`Database`] A named or unnamed key-value store within an environment.
//!   Opened via [`Transaction::open_db()`] or created via
//!   [`Transaction::create_db()`].
//! - [`Cursor`]: Enables iteration and positioned access within a database.
//!   Created via [`Transaction::cursor()`].
//!
//! # Feature Flags
//!
//! - `return-borrowed`: When enabled, iterators return borrowed data
//!   (`Cow::Borrowed`) whenever possible, avoiding allocations. This is faster
//!   but the data may change if the transaction modifies it later, which could
//!   trigger undefined behavior. When disabled (default), dirty pages in write
//!   transactions trigger copies for safety.
//! - `read-tx-timeouts`: Enables automatic timeout handling for read
//!   transactions that block writers. Useful for detecting stuck readers.
//!
//! # Custom Types with [`TableObject`]
//!
//! Implement [`TableObject`] to decode custom types directly from the
//! database:
//!
//! ```
//! # use std::borrow::Cow;
//! use signet_libmdbx::{TableObject, ReadResult, MdbxError};
//!
//! struct MyKey([u8; 32]);
//!
//! impl TableObject<'_> for MyKey {
//!     fn decode_borrow(data: Cow<'_, [u8]>) -> ReadResult<Self> {
//!         let arr: [u8; 32] = data.as_ref().try_into()
//!             .map_err(|_| MdbxError::DecodeErrorLenDiff)?;
//!         Ok(Self(arr))
//!     }
//! }
//! ```
//!
//! See the [`TableObject`] docs for more examples.
//!
//! # Provenance
//!
//! Forked from [reth-libmdbx], which was forked from an earlier Apache
//! licensed version of the `libmdbx-rs` crate. Original LMDB bindings from
//! [lmdb-rs].
//!
//! [libmdbx]: https://github.com/erthink/libmdbx
//! [reth-libmdbx]: https://github.com/paradigmxyz/reth
//! [lmdb-rs]: https://github.com/mozilla/lmdb-rs

#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    clippy::missing_const_for_fn,
    rustdoc::all
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub extern crate signet_mdbx_sys as ffi;

mod codec;
pub use codec::*;

#[cfg(feature = "read-tx-timeouts")]
pub use crate::sys::read_transactions::MaxReadTransactionDuration;

mod error;
pub use error::{MdbxError, MdbxResult, ReadError, ReadResult};

mod flags;
pub use flags::*;

mod sys;
pub use sys::{
    Environment, EnvironmentBuilder, EnvironmentKind, Geometry, HandleSlowReadersCallback,
    HandleSlowReadersReturnCode, Info, PageSize, Stat,
};

pub mod tx;
pub use tx::{CommitLatency, Cursor, Database, RO, RW, Transaction, TransactionKind, iter};

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::{ByteOrder, LittleEndian};
    use tempfile::tempdir;

    /// Regression test for <https://github.com/danburkert/lmdb-rs/issues/21>.
    /// This test reliably segfaults when run against lmdb compiled with opt
    /// level -O3 and newer GCC compilers.
    #[test]
    fn issue_21_regression() {
        const HEIGHT_KEY: [u8; 1] = [0];

        let dir = tempdir().unwrap();

        let env = {
            let mut builder = Environment::builder();
            builder.set_max_dbs(2);
            builder
                .set_geometry(Geometry { size: Some(1_000_000..1_000_000), ..Default::default() });
            builder.open(dir.path()).expect("open mdbx env")
        };

        for height in 0..1000 {
            let mut value = [0u8; 8];
            LittleEndian::write_u64(&mut value, height);
            let tx = env.begin_rw_txn().expect("begin_rw_txn");
            let index = tx.create_db(None, DatabaseFlags::DUP_SORT).expect("open index db");
            tx.put(index.dbi(), HEIGHT_KEY, value, WriteFlags::empty()).expect("tx.put");
            tx.commit().expect("tx.commit");
        }
    }
}
