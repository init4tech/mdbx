//! Common test infrastructure for dual transaction variant testing.
//!
//! This module provides traits that abstract over the two transaction
//! implementations (V1 shared and V2 single-threaded), allowing generic test
//! functions that work with either variant.
#![allow(missing_docs, dead_code)]
use signet_libmdbx::{
    Cursor, Database, DatabaseFlags, Environment, MdbxResult, ReadResult, Ro, RoSync, Rw, RwSync,
    Stat, TableObject, TransactionKind, TxSync, TxUnsync, WriteFlags, ffi,
    tx::{
        WriteMarker,
        aliases::{RoTxSync, RoTxUnsync, RwTxSync, RwTxUnsync},
    },
};

/// Trait for read-write transaction operations used in tests.
pub trait TestRwTxn: Sized {
    /// The kind
    type Kind: TransactionKind + WriteMarker;

    fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<Database>;
    fn open_db(&self, name: Option<&str>) -> MdbxResult<Database>;
    fn get<'a, T: TableObject<'a>>(
        &'a self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>>;
    fn put(&self, db: Database, key: &[u8], data: &[u8], flags: WriteFlags) -> MdbxResult<()>;
    fn append(&self, db: Database, key: &[u8], data: &[u8]) -> MdbxResult<()>;
    fn append_dup(&self, db: Database, key: &[u8], data: &[u8]) -> MdbxResult<()>;
    fn del(&self, db: Database, key: &[u8], data: Option<&[u8]>) -> MdbxResult<bool>;
    fn clear_db(&self, db: Database) -> MdbxResult<()>;
    fn commit(self) -> MdbxResult<()>;
    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, Self::Kind>>;
    fn db_stat(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat>;

    /// # Safety_by_dbi
    /// Caller must close all other Database and Cursor instances pointing to
    /// this dbi before calling.
    unsafe fn drop_db(&self, db: Database) -> MdbxResult<()>;
}

/// Trait for read-only transaction operations used in tests.
pub trait TestRoTxn: Sized {
    type Kind: TransactionKind;

    fn open_db(&self, name: Option<&str>) -> MdbxResult<Database>;
    fn get<'a, T: TableObject<'a>>(
        &'a self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>>;
    fn commit(self) -> MdbxResult<()>;
    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, Self::Kind>>;
    fn db_stat(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat>;
}

// =============================================================================_by_dbi
// V1 Transaction implementations
// =============================================================================

impl TestRwTxn for RwTxSync {
    type Kind = RwSync;

    fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<Database> {
        TxSync::create_db(self, name, flags)
    }

    fn open_db(&self, name: Option<&str>) -> MdbxResult<Database> {
        TxSync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        TxSync::get(self, dbi, key)
    }

    fn put(&self, db: Database, key: &[u8], data: &[u8], flags: WriteFlags) -> MdbxResult<()> {
        TxSync::put(self, db, key, data, flags)
    }

    fn append(&self, db: Database, key: &[u8], data: &[u8]) -> MdbxResult<()> {
        TxSync::append(self, db, key, data)
    }

    fn append_dup(&self, db: Database, key: &[u8], data: &[u8]) -> MdbxResult<()> {
        TxSync::append_dup(self, db, key, data)
    }

    fn del(&self, db: Database, key: &[u8], data: Option<&[u8]>) -> MdbxResult<bool> {
        TxSync::del(self, db, key, data)
    }

    fn clear_db(&self, db: Database) -> MdbxResult<()> {
        TxSync::clear_db(self, db)
    }

    fn commit(self) -> MdbxResult<()> {
        TxSync::commit(self).map(|_| ())
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, RwSync>> {
        TxSync::cursor(self, db)
    }

    fn db_stat(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        TxSync::db_stat_by_dbi(self, dbi)
    }

    unsafe fn drop_db(&self, db: Database) -> MdbxResult<()> {
        // SAFETY: Caller ensures no other references to dbi exist.
        unsafe { TxSync::drop_db(self, db) }
    }
}

impl TestRoTxn for RoTxSync {
    type Kind = RoSync;

    fn open_db(&self, name: Option<&str>) -> MdbxResult<Database> {
        TxSync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        TxSync::get(self, dbi, key)
    }

    fn commit(self) -> MdbxResult<()> {
        TxSync::commit(self).map(|_| ())
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, Self::Kind>> {
        TxSync::cursor(self, db)
    }

    fn db_stat(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        TxSync::db_stat_by_dbi(self, dbi)
    }
}

// =============================================================================
// V2 Transaction implementations
// =============================================================================

impl TestRwTxn for RwTxUnsync {
    type Kind = Rw;

    fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<Database> {
        TxUnsync::create_db(self, name, flags)
    }

    fn open_db(&self, name: Option<&str>) -> MdbxResult<Database> {
        TxUnsync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        TxUnsync::get(self, dbi, key)
    }

    fn put(&self, db: Database, key: &[u8], data: &[u8], flags: WriteFlags) -> MdbxResult<()> {
        TxUnsync::put(self, db, key, data, flags)
    }

    fn append(&self, db: Database, key: &[u8], data: &[u8]) -> MdbxResult<()> {
        TxUnsync::append(self, db, key, data)
    }

    fn append_dup(&self, db: Database, key: &[u8], data: &[u8]) -> MdbxResult<()> {
        TxUnsync::append_dup(self, db, key, data)
    }

    fn del(&self, db: Database, key: &[u8], data: Option<&[u8]>) -> MdbxResult<bool> {
        TxUnsync::del(self, db, key, data)
    }

    fn clear_db(&self, db: Database) -> MdbxResult<()> {
        TxUnsync::clear_db(self, db)
    }

    fn commit(self) -> MdbxResult<()> {
        TxUnsync::commit(self)
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, Rw>> {
        TxUnsync::cursor(self, db)
    }

    fn db_stat(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        TxUnsync::db_stat_by_dbi(self, dbi)
    }

    unsafe fn drop_db(&self, db: Database) -> MdbxResult<()> {
        // SAFETY: Caller ensures no other references to dbi exist.
        unsafe { TxUnsync::drop_db(self, db) }
    }
}

impl TestRoTxn for TxUnsync<Ro> {
    type Kind = Ro;

    fn open_db(&self, name: Option<&str>) -> MdbxResult<Database> {
        TxUnsync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        TxUnsync::get(self, dbi, key)
    }

    fn commit(self) -> MdbxResult<()> {
        TxUnsync::commit(self)
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, Self::Kind>> {
        TxUnsync::cursor(self, db)
    }

    fn db_stat(&self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        TxUnsync::db_stat_by_dbi(self, dbi)
    }
}

// =============================================================================
// Transaction factory functions
// =============================================================================

/// Factory for creating V1 transactions.
pub struct V1Factory;

impl V1Factory {
    pub fn begin_rw(env: &Environment) -> MdbxResult<RwTxSync> {
        env.begin_rw_sync()
    }

    pub fn begin_ro(env: &Environment) -> MdbxResult<RoTxSync> {
        env.begin_ro_sync()
    }
}

/// Factory for creating V2 transactions.
pub struct V2Factory;

impl V2Factory {
    pub fn begin_rw(env: &Environment) -> MdbxResult<RwTxUnsync> {
        env.begin_rw_unsync()
    }

    pub fn begin_ro(env: &Environment) -> MdbxResult<RoTxUnsync> {
        env.begin_ro_unsync()
    }
}

/// Macro to generate dual-variant tests.
///
/// This macro generates two test functions from a single test implementation:
/// one for V1 transactions and one for V2 transactions.
///
/// # Example
///
/// ```ignore
/// dual_test!(test_put_get, |env: &Environment| {
///     // This closure receives the environment and should return a result
///     // It will be run twice: once with V1 and once with V2 transactions
/// });
/// ```
#[macro_export]
macro_rules! dual_test {
    ($name:ident, $test_fn:expr) => {
        paste::paste! {
            #[test]
            fn [<$name _v1>]() {
                $test_fn(
                    common::V1Factory::begin_rw,
                    common::V1Factory::begin_ro,
                );
            }

            #[test]
            fn [<$name _v2>]() {
                $test_fn(
                    common::V2Factory::begin_rw,
                    common::V2Factory::begin_ro,
                );
            }
        }
    };
}
