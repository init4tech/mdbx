//! Transaction management and access.
//!
//! # Core Types (re-exported at crate root)
//!
//! - [`aliases::TxSync`] - Thread-safe synchronized transaction
//! - [`aliases::TxUnsync`] - Single-threaded unsynchronized transaction
//! - [`Cursor`] - Database cursor for navigating entries
//! - [`Database`] - Handle to an opened database
//! - [`Ro`], [`Rw`], [`RoSync`], [`RwSync`] - Transaction kind markers
//! - [`CommitLatency`] - Commit timing information
//!
//! # Type Aliases
//!
//! Convenience aliases for common transaction/cursor/iterator configurations
//! are available in [`aliases`]:
//! - [`aliases::RoTxSync`], [`aliases::RwTxSync`] - Synchronized transactions
//! - [`aliases::RoTxUnsync`], [`aliases::RwTxUnsync`] - Unsynchronized
//!   transactions
//! - [`aliases::RoCursorSync`], [`aliases::RwCursorSync`] - Cursors for
//!   synchronized transactions
//! - [`aliases::RoCursorUnsync`], [`aliases::RwCursorUnsync`] - Cursors for
//!   unsynchronized transactions
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

pub mod aliases;

pub mod cache;

mod cursor;
pub use cursor::Cursor;

mod database;
pub use database::Database;

pub mod iter;

mod kind;
pub use kind::{Ro, RoSync, Rw, RwSync, SyncKind, TransactionKind, WriteMarker, WriterKind};

mod lat;
pub use lat::CommitLatency;

/// Raw operations on transactions.
pub mod ops;

mod r#impl;
pub use r#impl::Tx;
