//! Public type aliases for transactions, cursors, and iterators.

use crate::{
    Ro, RoSync, Rw, RwSync,
    tx::{
        PtrSync, PtrUnsync,
        cursor::Cursor,
        r#impl::Tx,
        iter::{Iter, IterDupFixed, IterDupFixedOfKey},
    },
};
use std::{borrow::Cow, sync::Arc};

// --- Transaction aliases ---

/// Transaction type for synchronized access.
pub type TxSync<K> = Tx<K, Arc<PtrSync>>;

/// Transaction type for unsynchronized access.
pub type TxUnsync<K> = Tx<K, PtrUnsync>;

/// A synchronized read-only transaction.
pub type RoTxSync = TxSync<RoSync>;

/// A synchronized read-write transaction.
pub type RwTxSync = TxSync<RwSync>;

/// An unsynchronized read-only transaction.
pub type RoTxUnsync = TxUnsync<Ro>;

/// An unsynchronized read-write transaction.
pub type RwTxUnsync = TxUnsync<Rw>;

// SAFETY:
// - RoTxSync and RwTxSync use Arc<PtrSync> which is Send and Sync.
// - K::Cache is ALWAYS Send
// - TxMeta is ALWAYS Send
// - Moving an RO transaction between threads is safe as long as no concurrent
//   access occurs, which is guaranteed by being !Sync.
//
// NB: Send is correctly derived for RoTxSync and RwTxSync UNTIL
// you unsafe impl Sync for RoTxUnsync below. This is a quirk I did not know
// about.
unsafe impl Send for RoTxSync {}
unsafe impl Send for RwTxSync {}
unsafe impl Send for RoTxUnsync {}

// --- Cursor aliases ---

/// A read-only cursor for a synchronized transaction.
pub type RoCursorSync<'tx> = Cursor<'tx, RoSync>;

/// A read-write cursor for a synchronized transaction.
pub type RwCursorSync<'tx> = Cursor<'tx, RwSync>;

/// A read-only cursor for an unsynchronized transaction.
pub type RoCursorUnsync<'tx> = Cursor<'tx, Ro>;

/// A read-write cursor for an unsynchronized transaction.
pub type RwCursorUnsync<'tx> = Cursor<'tx, Rw>;

// --- Iterator aliases ---

/// Iterates over KV pairs in an MDBX database.
pub type IterKeyVals<'tx, 'cur, K, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    Iter<'tx, 'cur, K, Key, Value, { ffi::MDBX_NEXT }>;

/// An iterator over the key/value pairs in an MDBX `DUPSORT` with duplicate
/// keys, yielding the first value for each key.
///
/// See the [`Iter`] documentation for more details.
pub type IterDupKeys<'tx, 'cur, K, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    Iter<'tx, 'cur, K, Key, Value, { ffi::MDBX_NEXT_NODUP }>;

/// An iterator over the key/value pairs in an MDBX `DUPSORT`, yielding each
/// duplicate value for a specific key.
pub type IterDupVals<'tx, 'cur, K, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    Iter<'tx, 'cur, K, Key, Value, { ffi::MDBX_NEXT_DUP }>;

/// A key-value iterator for a synchronized read-only transaction.
pub type RoIterSync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    IterKeyVals<'tx, 'cur, RoSync, Key, Value>;

/// A key-value iterator for a synchronized read-write transaction.
pub type RwIterSync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    IterKeyVals<'tx, 'cur, RwSync, Key, Value>;

/// A key-value iterator for an unsynchronized read-only transaction.
pub type RoIterUnsync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    IterKeyVals<'tx, 'cur, Ro, Key, Value>;

/// A key-value iterator for an unsynchronized read-write transaction.
pub type RwIterUnsync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Cow<'tx, [u8]>> =
    IterKeyVals<'tx, 'cur, Rw, Key, Value>;

/// A flattening DUPFIXED iterator for a synchronized read-only transaction.
pub type RoDupFixedIterSync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Vec<u8>> =
    IterDupFixed<'tx, 'cur, RoSync, Key, Value>;

/// A flattening DUPFIXED iterator for a synchronized read-write transaction.
pub type RwDupFixedIterSync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Vec<u8>> =
    IterDupFixed<'tx, 'cur, RwSync, Key, Value>;

/// A flattening DUPFIXED iterator for an unsynchronized read-only transaction.
pub type RoDupFixedIterUnsync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Vec<u8>> =
    IterDupFixed<'tx, 'cur, Ro, Key, Value>;

/// A flattening DUPFIXED iterator for an unsynchronized read-write transaction.
pub type RwDupFixedIterUnsync<'tx, 'cur, Key = Cow<'tx, [u8]>, Value = Vec<u8>> =
    IterDupFixed<'tx, 'cur, Rw, Key, Value>;

/// A single-key DUPFIXED iterator for a synchronized read-only transaction.
pub type RoDupFixedIterOfKeySync<'tx, 'cur, Value = Vec<u8>> =
    IterDupFixedOfKey<'tx, 'cur, RoSync, Value>;

/// A single-key DUPFIXED iterator for a synchronized read-write transaction.
pub type RwDupFixedIterOfKeySync<'tx, 'cur, Value = Vec<u8>> =
    IterDupFixedOfKey<'tx, 'cur, RwSync, Value>;

/// A single-key DUPFIXED iterator for an unsynchronized read-only transaction.
pub type RoDupFixedIterOfKeyUnsync<'tx, 'cur, Value = Vec<u8>> =
    IterDupFixedOfKey<'tx, 'cur, Ro, Value>;

/// A single-key DUPFIXED iterator for an unsynchronized read-write transaction.
pub type RwDupFixedIterOfKeyUnsync<'tx, 'cur, Value = Vec<u8>> =
    IterDupFixedOfKey<'tx, 'cur, Rw, Value>;
