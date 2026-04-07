//! Utility functions for benchmarks.
#![allow(dead_code, unreachable_pub)]

use criterion::Criterion;
use signet_libmdbx::{
    Environment, Mode, SyncMode, WriteFlags,
    ffi::{MDBX_TXN_RDONLY, MDBX_env, MDBX_txn, mdbx_txn_begin_ex},
    tx::aliases::{RoTxSync, RoTxUnsync, RwTxSync, RwTxUnsync},
};
use std::ptr;
use tempfile::{TempDir, tempdir};

/// Returns true if `BENCH_FULL=1` is set in the environment.
pub fn is_bench_full() -> bool {
    std::env::var("BENCH_FULL").is_ok_and(|v| v == "1")
}

/// Quick criterion config: 10 samples, 1s warmup.
pub fn quick_config() -> Criterion {
    Criterion::default().sample_size(10).warm_up_time(std::time::Duration::from_secs(1))
}

/// Name of the named benchmark database.
pub const NAMED_DB: &str = "named_benchmark_db";

/// Generate a DB key string for testing.
pub fn get_key(n: u32) -> String {
    format!("key{n:029}")
}

/// Generate a 128-byte value for benchmarking.
pub fn get_data(n: u32) -> Vec<u8> {
    let seed = format!("data{n:010}");
    seed.as_bytes().iter().copied().cycle().take(128).collect()
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

/// 32-byte key with i as big-endian u32 in the first 4 bytes, rest zeroed.
pub fn bench_key(i: u32) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..4].copy_from_slice(&i.to_be_bytes());
    key
}

/// 128-byte value with i as little-endian u32 in the first 4 bytes, rest zeroed.
pub fn bench_value(i: u32) -> [u8; 128] {
    let mut value = [0u8; 128];
    value[..4].copy_from_slice(&i.to_le_bytes());
    value
}

/// Variable-size value encoding.
/// Repeats the little-endian u32 bytes of `i` across `size` bytes.
pub fn bench_value_sized(i: u32, size: usize) -> Vec<u8> {
    let bytes = i.to_le_bytes();
    (0..size).map(|j| bytes[j % 4]).collect()
}

/// Set up environment with N rows (default DB only).
/// Uses the default durable sync mode. Values are 128 bytes.
pub fn setup_bench_env(n: u32) -> (TempDir, Environment) {
    setup_bench_env_with_max_readers(n, None)
}

/// Set up environment with N rows using variable-size encoding.
/// Uses the default durable sync mode.
pub fn setup_bench_env_sized(n: u32, value_size: usize) -> (TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..n {
            txn.put(db, bench_key(i), bench_value_sized(i, value_size), WriteFlags::empty())
                .unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
}

/// Set up environment with N rows using SafeNoSync mode (no fsync).
pub fn setup_bench_env_nosync(n: u32) -> (TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder()
        .set_flags(Mode::ReadWrite { sync_mode: SyncMode::SafeNoSync }.into())
        .open(dir.path())
        .unwrap();
    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..n {
            txn.put(db, bench_key(i), bench_value(i), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
}

/// Set up environment with N rows and a custom max reader count.
/// Pass [`None`] for the mdbx default (126).
pub fn setup_bench_env_with_max_readers(
    n: u32,
    max_readers: Option<u64>,
) -> (TempDir, Environment) {
    let dir = tempdir().unwrap();
    let mut builder = Environment::builder();
    if let Some(max) = max_readers {
        builder.set_max_readers(max);
    }
    let env = builder.open(dir.path()).unwrap();
    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..n {
            txn.put(db, bench_key(i), bench_value(i), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
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
