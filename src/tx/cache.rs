//! Caches for [`Database`] info and cursor pointers, used by the [`TxSync`]
//! and [`TxUnsync`] types.
//!
//! This module defines cache types for storing database handles and cached
//! cursor pointers within transactions. The cache is co-located with the
//! transaction pointer (in [`PtrSync`]/[`PtrUnsync`]) so that its lifetime
//! is bound to the transaction's; this guarantees cached cursors are closed
//! exactly once, before the transaction is aborted or committed, even when
//! a `TxSync` is cloned across threads.
//!
//! The container is [`DbCache`], used either:
//! - inline behind a [`RefCell`] for unsynchronized transactions, or
//! - inline behind a [`parking_lot::Mutex`] for synchronized transactions
//!   (the same mutex that serialises raw txn-pointer access).
//!
//! [`TxSync`]: crate::tx::aliases::TxSync
//! [`TxUnsync`]: crate::tx::aliases::TxUnsync
//! [`PtrSync`]: crate::tx::PtrSync
//! [`PtrUnsync`]: crate::tx::PtrUnsync

use crate::Database;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::{
    cell::RefCell,
    hash::{Hash, Hasher},
};

/// Cache trait for transaction-local database handles and cursors.
///
/// Implemented on the concrete container types that wrap a [`DbCache`]
/// (e.g. [`Mutex<DbCache>`] for synchronized transactions and
/// [`RefCell<DbCache>`] for unsynchronized ones).
pub trait Cache: std::fmt::Debug {
    /// Read a database entry from the cache.
    fn read_db(&self, name_hash: u64) -> Option<Database>;

    /// Write a database entry to the cache.
    fn write_db(&self, db: CachedDb);

    /// Remove a database entry from the cache by dbi.
    fn remove_dbi(&self, dbi: ffi::MDBX_dbi);

    /// Take a cached cursor for the given DBI, if one exists.
    fn take_cursor(&self, dbi: ffi::MDBX_dbi) -> Option<*mut ffi::MDBX_cursor>;

    /// Return a cursor to the cache for later reuse.
    fn return_cursor(&self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor);

    /// Drain all cached cursors, returning their raw pointers.
    /// The caller is responsible for closing them via FFI.
    fn drain_cursors(&self) -> SmallVec<[*mut ffi::MDBX_cursor; 8]>;

    /// Drain cached cursors for a specific DBI, returning their raw pointers.
    /// The caller is responsible for closing them via FFI.
    fn drain_cursors_for_dbi(&self, dbi: ffi::MDBX_dbi) -> SmallVec<[*mut ffi::MDBX_cursor; 8]>;

    /// Returns the total number of cached cursors across all DBIs.
    #[cfg(test)]
    fn cursor_count(&self) -> usize;

    /// Injects a raw cursor pointer into the cache for the given DBI.
    ///
    /// # Safety
    ///
    /// - `cursor` must either be a valid MDBX cursor pointer bound to the
    ///   enclosing transaction, or a pointer whose `mdbx_cursor_close2`
    ///   behaviour the caller accepts (tests that trigger failure paths
    ///   knowingly inject unusual values here).
    #[cfg(test)]
    unsafe fn inject_cursor(&self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor);
}

/// Cached database entry.
///
/// Uses hash-only comparison since 64-bit hash collisions are negligible
/// for practical database counts.
#[derive(Debug, Clone, Copy)]
pub struct CachedDb {
    /// Hash of database name (None hashes distinctly from any string).
    name_hash: u64,
    /// The cached database (dbi + flags).
    db: Database,
}

impl CachedDb {
    /// Creates a new cached database entry.
    pub(crate) fn new(name: Option<&str>, db: Database) -> Self {
        let name_hash = Self::hash_name(name);
        Self { name_hash, db }
    }

    #[inline]
    pub(crate) fn hash_name(name: Option<&str>) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        name.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<CachedDb> for Database {
    fn from(value: CachedDb) -> Self {
        value.db
    }
}

/// Simple cache container for database handles and cursor pointers.
///
/// Uses inline storage for the common case (most apps use < 16 databases).
#[derive(Debug)]
pub struct DbCache {
    dbs: SmallVec<[CachedDb; 16]>,
    cursors: SmallVec<[(ffi::MDBX_dbi, *mut ffi::MDBX_cursor); 8]>,
}

// SAFETY: DbCache contains `*mut ffi::MDBX_cursor` which is `!Send + !Sync`.
// These are raw MDBX cursor pointers bound to a transaction, not a thread.
// `Cursor` itself is already `Send + Sync` (see cursor.rs), so caching the
// same pointers here introduces no new unsoundness. All access to these
// pointers is mediated by `RefCell` (unsync path) or `RwLock` (sync path),
// ensuring no concurrent mutation.
unsafe impl Send for DbCache {}
unsafe impl Sync for DbCache {}

impl Default for DbCache {
    fn default() -> Self {
        Self { dbs: SmallVec::new(), cursors: SmallVec::new() }
    }
}

impl DbCache {
    /// Read a database entry from the cache.
    fn read_db(&self, name_hash: u64) -> Option<Database> {
        self.dbs.iter().find(|e| e.name_hash == name_hash).map(|e| e.db)
    }

    /// Write a database entry to the cache.
    fn write_db(&mut self, db: CachedDb) {
        if self.dbs.iter().any(|e| e.name_hash == db.name_hash) {
            return;
        }
        self.dbs.push(db);
    }

    /// Remove a database entry from the cache by dbi.
    fn remove_dbi(&mut self, dbi: ffi::MDBX_dbi) {
        self.dbs.retain(|entry| entry.db.dbi() != dbi);
    }

    /// Take a cached cursor for the given DBI, if one exists.
    fn take_cursor(&mut self, dbi: ffi::MDBX_dbi) -> Option<*mut ffi::MDBX_cursor> {
        self.cursors.iter().position(|(d, _)| *d == dbi).map(|i| self.cursors.swap_remove(i).1)
    }

    /// Return a cursor to the cache for later reuse.
    fn return_cursor(&mut self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor) {
        self.cursors.push((dbi, cursor));
    }

    /// Drain all cached cursors, returning their raw pointers.
    pub(crate) fn drain_cursors(&mut self) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        self.cursors.drain(..).map(|(_, c)| c).collect()
    }

    /// Drain cached cursors for a specific DBI, returning their raw pointers.
    fn drain_cursors_for_dbi(
        &mut self,
        dbi: ffi::MDBX_dbi,
    ) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        let mut drained = SmallVec::new();
        self.cursors.retain(|(d, c)| {
            if *d == dbi {
                drained.push(*c);
                false
            } else {
                true
            }
        });
        drained
    }
}

impl Cache for Mutex<DbCache> {
    fn read_db(&self, name_hash: u64) -> Option<Database> {
        self.lock().read_db(name_hash)
    }

    fn write_db(&self, db: CachedDb) {
        self.lock().write_db(db);
    }

    fn remove_dbi(&self, dbi: ffi::MDBX_dbi) {
        self.lock().remove_dbi(dbi);
    }

    fn take_cursor(&self, dbi: ffi::MDBX_dbi) -> Option<*mut ffi::MDBX_cursor> {
        self.lock().take_cursor(dbi)
    }

    fn return_cursor(&self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor) {
        self.lock().return_cursor(dbi, cursor);
    }

    fn drain_cursors(&self) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        self.lock().drain_cursors()
    }

    fn drain_cursors_for_dbi(&self, dbi: ffi::MDBX_dbi) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        self.lock().drain_cursors_for_dbi(dbi)
    }

    #[cfg(test)]
    fn cursor_count(&self) -> usize {
        self.lock().cursors.len()
    }

    #[cfg(test)]
    unsafe fn inject_cursor(&self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor) {
        self.lock().cursors.push((dbi, cursor));
    }
}

impl Cache for RefCell<DbCache> {
    /// Read a database entry from the cache.
    fn read_db(&self, name_hash: u64) -> Option<Database> {
        self.borrow().read_db(name_hash)
    }

    /// Write a database entry to the cache.
    fn write_db(&self, db: CachedDb) {
        self.borrow_mut().write_db(db);
    }

    /// Remove a database entry from the cache by dbi.
    fn remove_dbi(&self, dbi: ffi::MDBX_dbi) {
        self.borrow_mut().remove_dbi(dbi);
    }

    fn take_cursor(&self, dbi: ffi::MDBX_dbi) -> Option<*mut ffi::MDBX_cursor> {
        self.borrow_mut().take_cursor(dbi)
    }

    fn return_cursor(&self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor) {
        self.borrow_mut().return_cursor(dbi, cursor);
    }

    fn drain_cursors(&self) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        self.borrow_mut().drain_cursors()
    }

    fn drain_cursors_for_dbi(&self, dbi: ffi::MDBX_dbi) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        self.borrow_mut().drain_cursors_for_dbi(dbi)
    }

    #[cfg(test)]
    fn cursor_count(&self) -> usize {
        self.borrow().cursors.len()
    }

    #[cfg(test)]
    unsafe fn inject_cursor(&self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor) {
        self.borrow_mut().cursors.push((dbi, cursor));
    }
}
