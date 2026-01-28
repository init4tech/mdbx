//! Safe access to database entries.
//!
//! This module provides abstractions for working with database entries,
//! including serialization/deserialization via the [`TableObject`] trait
//! and safe views of borrowed data through [`TxView`].
mod codec;
pub use codec::{ObjectLength, TableObject, TableObjectOwned};

mod view;
pub use view::TxView;

use crate::TransactionKind;

/// Synchronized table object view tied to a synchronized transaction.
pub type SyncView<'tx, K, T> = TxView<'tx, crate::tx::PtrSyncInner<K>, T>;

/// Unsynchronized table object view tied to an unsynchronized transaction.
pub type TableViewUnsync<'tx, K, T> = TxView<'tx, <K as TransactionKind>::UnsyncAccess, T>;

/// Synchronized key-value view tied to a synchronized transaction.
pub type SyncKvView<'tx, Kind, Key, Value> = (SyncView<'tx, Kind, Key>, SyncView<'tx, Kind, Value>);

/// Unsynchronized key-value view tied to an unsynchronized transaction.
pub type UnsyncKvView<'tx, Kind, Key, Value> =
    (TableViewUnsync<'tx, Kind, Key>, TableViewUnsync<'tx, Kind, Value>);

/// Key-value view type.
pub type KvView<'tx, A, Key, Value> = (TxView<'tx, A, Key>, TxView<'tx, A, Value>);

/// Optional KV pair view type.
pub type KvOpt<'tx, A, Key, Value> = Option<KvView<'tx, A, Key, Value>>;
