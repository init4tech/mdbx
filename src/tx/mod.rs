//! Transaction management and access.
//!
//! # Core Types (re-exported at crate root)
//!
//! - [`TxSync`] - Thread-safe synchronized transaction
//! - [`TxUnsync`] - Single-threaded unsynchronized transaction
//! - [`Cursor`] - Database cursor for navigating entries
//! - [`Database`] - Handle to an opened database
//! - [`Ro`], [`Rw`], [`RoSync`], [`RwSync`] - Transaction kind markers
//! - [`CommitLatency`] - Commit timing information
//!
//! # Type Aliases
//!
//! Convenience aliases for common transaction/cursor configurations:
//! - [`RoTxSync`], [`RwTxSync`] - Synchronized transactions
//! - [`RoTxUnsync`], [`RwTxUnsync`] - Unsynchronized transactions
//! - [`RoCursorSync`], [`RwCursorSync`] - Cursors for synchronized transactions
//! - [`RoCursorUnsync`], [`RwCursorUnsync`] - Cursors for unsynchronized
//!   transactions
//!
//! # Advanced: Writing Generic Code
//!
//! For users writing generic code over cursors or transactions, we recommend
//! reviewing the [`TransactionKind`], [`WriterKind`], and [`SyncKind`] traits,
//! as well as exploring the bounds on impl blocks for the various transaction
//! and cursor types.

mod assertions;

mod access;
pub use access::{PtrSync, PtrUnsync, TxPtrAccess};

pub mod cache;

mod cursor;
pub use cursor::{Cursor, RoCursorSync, RoCursorUnsync, RwCursorSync, RwCursorUnsync};

mod database;
pub use database::Database;

pub mod iter;
pub use iter::{RoIterSync, RoIterUnsync, RwIterSync, RwIterUnsync};

mod kind;
pub use kind::{Ro, RoSync, Rw, RwSync, SyncKind, TransactionKind, WriteMarker, WriterKind};

mod lat;
pub use lat::CommitLatency;

/// Raw operations on transactions.
pub mod ops;

mod r#impl;
pub use r#impl::{RoTxSync, RoTxUnsync, RwTxSync, RwTxUnsync, Tx, TxSync, TxUnsync};
