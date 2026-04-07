//! Caches for [`Database`] info, used by the [`TxSync`] and [`TxUnsync`] types.
//!
//! This module defines cache types for storing database handles within
//! transactions. Caches improve performance by avoiding repeated lookups of
//! database information.
//!
//! The primary caches are:
//! - [`DbCache`]: A simple inline cache using `SmallVec` for efficient storage
//!   of a small number of database handles. Used in unsynchronized
//!   transactions via [`RefCell`].
//! - [`SharedCache`]: A thread-safe cache using `Arc<RwLock<...>>` for
//!   synchronized transactions.
//!
//! [`TxSync`]: crate::tx::aliases::TxSync
//! [`TxUnsync`]: crate::tx::aliases::TxUnsync

use crate::Database;
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::{
    cell::RefCell,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// Cache trait for transaction-local database handles and cursors.
///
/// This is used by the [`SyncKind`] trait to define the cache type for each
/// transaction kind.
///
/// [`SyncKind`]: crate::tx::kind::SyncKind
pub trait Cache: Clone + Default + std::fmt::Debug {
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

impl Clone for DbCache {
    fn clone(&self) -> Self {
        Self { dbs: self.dbs.clone(), cursors: SmallVec::new() }
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
    fn drain_cursors(&mut self) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        self.cursors.drain(..).map(|(_, c)| c).collect()
    }
}

/// Simple cache container for database handles.
///
/// Uses inline storage for the common case (most apps use < 16 databases).
#[derive(Debug, Clone)]
pub struct SharedCache {
    cache: Arc<RwLock<DbCache>>,
}

impl SharedCache {
    /// Creates a new empty cache.
    fn new() -> Self {
        Self { cache: Arc::new(RwLock::new(DbCache::default())) }
    }

    /// Returns a read guard to the cache.
    fn read(&self) -> parking_lot::RwLockReadGuard<'_, DbCache> {
        self.cache.read()
    }

    /// Returns a write guard to the cache.
    fn write(&self) -> parking_lot::RwLockWriteGuard<'_, DbCache> {
        self.cache.write()
    }
}

impl Cache for SharedCache {
    /// Read a database entry from the cache.
    fn read_db(&self, name_hash: u64) -> Option<Database> {
        self.read().read_db(name_hash)
    }

    /// Write a database entry to the cache.
    fn write_db(&self, db: CachedDb) {
        self.write().write_db(db);
    }

    /// Remove a database entry from the cache by dbi.
    fn remove_dbi(&self, dbi: ffi::MDBX_dbi) {
        self.write().remove_dbi(dbi);
    }

    fn take_cursor(&self, dbi: ffi::MDBX_dbi) -> Option<*mut ffi::MDBX_cursor> {
        self.write().take_cursor(dbi)
    }

    fn return_cursor(&self, dbi: ffi::MDBX_dbi, cursor: *mut ffi::MDBX_cursor) {
        self.write().return_cursor(dbi, cursor);
    }

    fn drain_cursors(&self) -> SmallVec<[*mut ffi::MDBX_cursor; 8]> {
        self.write().drain_cursors()
    }
}

impl Default for SharedCache {
    fn default() -> Self {
        Self::new()
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
}
