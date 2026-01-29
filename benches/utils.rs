//! Utility functions for benchmarks.
#![allow(dead_code, unreachable_pub)]

use signet_libmdbx::{
    Environment, WriteFlags,
    ffi::{MDBX_TXN_RDONLY, MDBX_env, MDBX_txn, mdbx_txn_begin_ex},
    tx::aliases::{RoTxSync, RoTxUnsync, RwTxSync, RwTxUnsync},
};
use std::ptr;
use tempfile::{TempDir, tempdir};

/// Name of the named benchmark database.
pub const NAMED_DB: &str = "named_benchmark_db";

/// Generate a DB key string for testing.
pub fn get_key(n: u32) -> String {
    format!("key{n}")
}

// Generate a DB data string for testing.
pub fn get_data(n: u32) -> String {
    format!("data{n}")
}

// Raw transaction utilities

/// Create a raw read-only transaction from an environment pointer.
///
/// # Safety
///
/// The caller must ensure `env` is a valid environment pointer.
#[inline(always)]
pub unsafe fn create_ro_raw(env: *mut MDBX_env) -> *mut MDBX_txn {
    let mut txn: *mut MDBX_txn = ptr::null_mut();
    // SAFETY: Caller guarantees env is valid.
    unsafe { mdbx_txn_begin_ex(env, ptr::null_mut(), MDBX_TXN_RDONLY, &mut txn, ptr::null_mut()) };
    txn
}

/// Create a raw read-write transaction from an environment pointer.
///
/// # Safety
///
/// The caller must ensure `env` is a valid environment pointer.
#[inline(always)]
pub unsafe fn create_rw_raw(env: *mut MDBX_env) -> *mut MDBX_txn {
    let mut txn: *mut MDBX_txn = ptr::null_mut();
    // SAFETY: Caller guarantees env is valid.
    unsafe { mdbx_txn_begin_ex(env, ptr::null_mut(), 0, &mut txn, ptr::null_mut()) };
    txn
}

// Sync transaction utilities

/// Create a read-only synchronized transaction.
pub fn create_ro_sync(env: &Environment) -> RoTxSync {
    env.begin_ro_sync().unwrap()
}

/// Create a read-write synchronized transaction.
pub fn create_rw_sync(env: &Environment) -> RwTxSync {
    env.begin_rw_sync().unwrap()
}

// Unsync transaction utilities

/// Create a read-only unsynchronized transaction.
pub fn create_ro_unsync(env: &Environment) -> RoTxUnsync {
    env.begin_ro_unsync().unwrap()
}

/// Create a read-write unsynchronized transaction.
pub fn create_rw_unsync(env: &Environment) -> RwTxUnsync {
    env.begin_rw_unsync().unwrap()
}

/// Create a temporary benchmark database with the specified number of rows.
pub fn setup_bench_db(num_rows: u32) -> (TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..num_rows {
            txn.put(db, get_key(i), get_data(i), WriteFlags::empty()).unwrap();
        }

        let named_db = txn.create_db(Some(NAMED_DB), Default::default()).unwrap();
        for i in 0..num_rows {
            txn.put(named_db, get_key(i), get_data(i), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
}
