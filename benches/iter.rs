#![allow(missing_docs)]
mod utils;

use crate::utils::{create_ro_sync, create_ro_unsync};
use criterion::{Criterion, criterion_group, criterion_main};
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use std::hint::black_box;
use tempfile::{TempDir, tempdir};

const VALUE_SIZE: usize = 100;
const NUM_VALUES: u32 = 2000;

const DB_NAME: &str = "dupfixed_bench";

/// Setup a DUPFIXED database with NUM_VALUES 100-byte values under a single key.
fn setup_dupfixed_db() -> (TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(1).open(dir.path()).unwrap();

    let txn = env.begin_rw_unsync().unwrap();
    let db =
        txn.create_db(Some(DB_NAME), DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

    // Insert NUM_VALUES with incrementing content
    for i in 0..NUM_VALUES {
        let mut value = [0u8; VALUE_SIZE];
        value[..4].copy_from_slice(&i.to_le_bytes());
        txn.put(db, b"key", value, WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    (dir, env)
}

/// Benchmark: iter_dupfixed (batched page fetching).
fn bench_iter_dupfixed(c: &mut Criterion) {
    let (_dir, env) = setup_dupfixed_db();
    let txn = create_ro_unsync(&env);
    let db = txn.open_db(Some(DB_NAME)).unwrap();

    c.bench_function("unsync::iter::dupfixed::batched", |b| {
        b.iter(|| {
            let mut cursor = txn.cursor(db).unwrap();
            let mut count = 0u32;
            for result in cursor.iter_dupfixed_start::<[u8; 3], [u8; VALUE_SIZE]>().unwrap() {
                let (_key, value) = result.unwrap();
                black_box(value);
                count += 1;
            }
            assert_eq!(count, NUM_VALUES);
        })
    });
}

/// Benchmark: simple next() iteration.
fn bench_iter_simple(c: &mut Criterion) {
    let (_dir, env) = setup_dupfixed_db();
    let txn = create_ro_unsync(&env);
    let db = txn.open_db(Some(DB_NAME)).unwrap();

    c.bench_function("unsync::iter::dupfixed::simple_next", |b| {
        b.iter(|| {
            let mut cursor = txn.cursor(db).unwrap();
            let mut count = 0u32;
            for result in cursor.iter_start::<[u8; 3], [u8; VALUE_SIZE]>().unwrap() {
                let (_key, value) = result.unwrap();
                black_box(value);
                count += 1;
            }
            assert_eq!(count, NUM_VALUES);
        })
    });
}

/// Benchmark: iter_dupfixed (batched page fetching).
fn bench_iter_dupfixed_sync(c: &mut Criterion) {
    let (_dir, env) = setup_dupfixed_db();
    let txn = create_ro_sync(&env);
    let db = txn.open_db(Some(DB_NAME)).unwrap();

    c.bench_function("sync::iter::dupfixed::batched", |b| {
        b.iter(|| {
            let mut cursor = txn.cursor(db).unwrap();
            let mut count = 0u32;
            for result in cursor.iter_dupfixed_start::<[u8; 3], [u8; VALUE_SIZE]>().unwrap() {
                let (_key, value) = result.unwrap();
                black_box(value);
                count += 1;
            }
            assert_eq!(count, NUM_VALUES);
        })
    });
}

/// Benchmark: simple next() iteration.
fn bench_iter_simple_sync(c: &mut Criterion) {
    let (_dir, env) = setup_dupfixed_db();
    let txn = create_ro_sync(&env);
    let db = txn.open_db(Some(DB_NAME)).unwrap();

    c.bench_function("sync::iter::dupfixed::simple_next", |b| {
        b.iter(|| {
            let mut cursor = txn.cursor(db).unwrap();
            let mut count = 0u32;
            for result in cursor.iter_start::<[u8; 3], [u8; VALUE_SIZE]>().unwrap() {
                let (_key, value) = result.unwrap();
                black_box(value);
                count += 1;
            }
            assert_eq!(count, NUM_VALUES);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_iter_dupfixed, bench_iter_simple,
              bench_iter_dupfixed_sync, bench_iter_simple_sync,
}

criterion_main!(benches);
