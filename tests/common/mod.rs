//! Common test infrastructure for dual transaction variant testing.
//!
//! This module provides traits that abstract over the two transaction
//! implementations (V1 shared and V2 single-threaded), allowing generic test
//! functions that work with either variant.
#![allow(missing_docs, dead_code)]
use signet_libmdbx::{
    Cursor, Database, DatabaseFlags, Environment, MdbxResult, RO, RW, ReadResult, Stat,
    TableObject, TxSync, WriteFlags, ffi,
    tx::{PtrSyncInner, RoGuard, RwUnsync, TxPtrAccess, unsync},
};

/// Trait for read-write transaction operations used in tests.
pub trait TestRwTxn: Sized {
    /// The cursor access type for this transaction.
    type CursorAccess: TxPtrAccess;

    fn create_db(&mut self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<Database>;
    fn open_db(&mut self, name: Option<&str>) -> MdbxResult<Database>;
    fn get<'a, T: TableObject<'a>>(
        &'a mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>>;
    fn put(
        &mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
        data: &[u8],
        flags: WriteFlags,
    ) -> MdbxResult<()>;
    fn del(&mut self, dbi: ffi::MDBX_dbi, key: &[u8], data: Option<&[u8]>) -> MdbxResult<bool>;
    fn clear_db(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<()>;
    fn commit(self) -> MdbxResult<()>;
    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, RW, Self::CursorAccess>>;
    fn db_stat(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat>;

    /// # Safety
    /// Caller must close all other Database and Cursor instances pointing to
    /// this dbi before calling.
    unsafe fn drop_db(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<()>;
}

/// Trait for read-only transaction operations used in tests.
pub trait TestRoTxn: Sized {
    /// The cursor access type for this transaction.
    type CursorAccess: TxPtrAccess;

    fn open_db(&mut self, name: Option<&str>) -> MdbxResult<Database>;
    fn get<'a, T: TableObject<'a>>(
        &'a mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>>;
    fn commit(self) -> MdbxResult<()>;
    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, RO, Self::CursorAccess>>;
    fn db_stat(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat>;
}

// =============================================================================
// V1 Transaction implementations
// =============================================================================

impl TestRwTxn for TxSync<RW> {
    type CursorAccess = PtrSyncInner<RW>;

    fn create_db(&mut self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<Database> {
        TxSync::create_db(self, name, flags)
    }

    fn open_db(&mut self, name: Option<&str>) -> MdbxResult<Database> {
        TxSync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        TxSync::get(self, dbi, key)
    }

    fn put(
        &mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
        data: &[u8],
        flags: WriteFlags,
    ) -> MdbxResult<()> {
        TxSync::put(self, dbi, key, data, flags)
    }

    fn del(&mut self, dbi: ffi::MDBX_dbi, key: &[u8], data: Option<&[u8]>) -> MdbxResult<bool> {
        TxSync::del(self, dbi, key, data)
    }

    fn clear_db(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        TxSync::clear_db(self, dbi)
    }

    fn commit(self) -> MdbxResult<()> {
        TxSync::commit(self).map(|_| ())
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, RW, Self::CursorAccess>> {
        TxSync::cursor(self, db)
    }

    fn db_stat(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        TxSync::db_stat(self, dbi)
    }

    unsafe fn drop_db(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        // SAFETY: Caller ensures no other references to dbi exist.
        unsafe { TxSync::drop_db(self, dbi) }
    }
}

impl TestRoTxn for TxSync<RO> {
    type CursorAccess = PtrSyncInner<RO>;

    fn open_db(&mut self, name: Option<&str>) -> MdbxResult<Database> {
        TxSync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        TxSync::get(self, dbi, key)
    }

    fn commit(self) -> MdbxResult<()> {
        TxSync::commit(self).map(|_| ())
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, RO, Self::CursorAccess>> {
        TxSync::cursor(self, db)
    }

    fn db_stat(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        TxSync::db_stat(self, dbi)
    }
}

// =============================================================================
// V2 Transaction implementations
// =============================================================================

impl TestRwTxn for unsync::TxUnsync<RW> {
    type CursorAccess = RwUnsync;

    fn create_db(&mut self, name: Option<&str>, flags: DatabaseFlags) -> MdbxResult<Database> {
        unsync::TxUnsync::create_db(self, name, flags)
    }

    fn open_db(&mut self, name: Option<&str>) -> MdbxResult<Database> {
        unsync::TxUnsync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        unsync::TxUnsync::get(self, dbi, key)
    }

    fn put(
        &mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
        data: &[u8],
        flags: WriteFlags,
    ) -> MdbxResult<()> {
        unsync::TxUnsync::put(self, dbi, key, data, flags)
    }

    fn del(&mut self, dbi: ffi::MDBX_dbi, key: &[u8], data: Option<&[u8]>) -> MdbxResult<bool> {
        unsync::TxUnsync::del(self, dbi, key, data)
    }

    fn clear_db(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        unsync::TxUnsync::clear_db(self, dbi)
    }

    fn commit(self) -> MdbxResult<()> {
        unsync::TxUnsync::commit(self)
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, RW, Self::CursorAccess>> {
        unsync::TxUnsync::cursor(self, db)
    }

    fn db_stat(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        unsync::TxUnsync::db_stat(self, dbi)
    }

    unsafe fn drop_db(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<()> {
        // SAFETY: Caller ensures no other references to dbi exist.
        unsafe { unsync::TxUnsync::drop_db(self, dbi) }
    }
}

impl TestRoTxn for unsync::TxUnsync<RO> {
    type CursorAccess = RoGuard;

    fn open_db(&mut self, name: Option<&str>) -> MdbxResult<Database> {
        unsync::TxUnsync::open_db(self, name)
    }

    fn get<'a, T: TableObject<'a>>(
        &'a mut self,
        dbi: ffi::MDBX_dbi,
        key: &[u8],
    ) -> ReadResult<Option<T>> {
        unsync::TxUnsync::get(self, dbi, key)
    }

    fn commit(self) -> MdbxResult<()> {
        unsync::TxUnsync::commit(self)
    }

    fn cursor(&self, db: Database) -> MdbxResult<Cursor<'_, RO, Self::CursorAccess>> {
        unsync::TxUnsync::cursor(self, db)
    }

    fn db_stat(&mut self, dbi: ffi::MDBX_dbi) -> MdbxResult<Stat> {
        unsync::TxUnsync::db_stat(self, dbi)
    }
}

// =============================================================================
// Transaction factory functions
// =============================================================================

/// Factory for creating V1 transactions.
pub struct V1Factory;

impl V1Factory {
    pub fn begin_rw(env: &Environment) -> MdbxResult<TxSync<RW>> {
        env.begin_rw_txn()
    }

    pub fn begin_ro(env: &Environment) -> MdbxResult<TxSync<RO>> {
        env.begin_ro_txn()
    }
}

/// Factory for creating V2 transactions.
pub struct V2Factory;

impl V2Factory {
    pub fn begin_rw(env: &Environment) -> MdbxResult<unsync::TxUnsync<RW>> {
        env.begin_rw_unsync()
    }

    pub fn begin_ro(env: &Environment) -> MdbxResult<unsync::TxUnsync<RO>> {
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
