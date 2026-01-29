#![allow(missing_docs)]

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use tempfile::{TempDir, tempdir};

const VALUE_SIZE: usize = 100;
const DB_NAME: &str = "deletion_bench";

/// Setup a DUPSORT database with the specified number of 100-byte values under a single key.
fn setup_deletion_db(num_values: u32) -> (TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(1).open(dir.path()).unwrap();

    let txn = env.begin_rw_unsync().unwrap();
    let db = txn.create_db(Some(DB_NAME), DatabaseFlags::DUP_SORT).unwrap();

    for i in 0..num_values {
        let mut value = [0u8; VALUE_SIZE];
        value[..4].copy_from_slice(&i.to_le_bytes());
        txn.put(db, b"key", value, WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    (dir, env)
}

/// Benchmark: del_all_dups (single call bulk deletion).
fn bench_del_all_dups(c: &mut Criterion) {
    for num_values in [100, 2000, 10000] {
        c.bench_function(&format!("del::del_all_dups::{num_values}"), |b| {
            b.iter_batched(
                || setup_deletion_db(num_values),
                |(_dir, env)| {
                    let txn = env.begin_rw_unsync().unwrap();
                    let db = txn.open_db(Some(DB_NAME)).unwrap();
                    {
                        let mut cursor = txn.cursor(db).unwrap();
                        cursor.set::<()>(b"key").unwrap();
                        cursor.del_all_dups().unwrap();
                    }
                    txn.commit().unwrap();
                },
                BatchSize::SmallInput,
            )
        });
    }
}

/// Benchmark: loop deletion (delete each entry individually).
fn bench_del_loop(c: &mut Criterion) {
    for num_values in [100, 2000, 10000] {
        c.bench_function(&format!("del::loop::{num_values}"), |b| {
            b.iter_batched(
                || setup_deletion_db(num_values),
                |(_dir, env)| {
                    let txn = env.begin_rw_unsync().unwrap();
                    let db = txn.open_db(Some(DB_NAME)).unwrap();
                    {
                        let mut cursor = txn.cursor(db).unwrap();
                        cursor.set::<()>(b"key").unwrap();
                        while cursor.get_current::<(), ()>().unwrap().is_some() {
                            cursor.del().unwrap();
                        }
                    }
                    txn.commit().unwrap();
                },
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_del_all_dups, bench_del_loop,
}

criterion_main!(benches);
