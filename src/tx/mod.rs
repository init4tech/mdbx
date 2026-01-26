//! Transaction management and access.
//!
//! Generally, users will interact with transactions via the
//! [`Transaction`] type, which provides a safe interface for both read-only
//! and read-write transactions. This module contains lower-level types for
//! managing transaction pointers and their access patterns.

mod access;
pub(crate) use access::{PtrSync, RoGuard, RwUnsync, TxPtrAccess};

mod cache;
pub(crate) use cache::{CachedDb, SharedCache};

mod cursor;
pub use cursor::Cursor;

mod database;
pub use database::Database;

pub mod iter;

mod kind;
pub use kind::{RO, RW, TransactionKind};

/// Raw operations on transactions.
pub mod ops;

mod transaction;
#[allow(unused_imports)] // this is used in some features
pub use transaction::{CommitLatency, Transaction};

pub mod transaction_2;

/// The default maximum duration of a read transaction.
#[cfg(feature = "read-tx-timeouts")]
pub const DEFAULT_MAX_READ_TRANSACTION_DURATION: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);
