#![allow(missing_docs, dead_code)]

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{SeedableRng, prelude::SliceRandom, rngs::StdRng};
use signet_libmdbx::{Environment, ObjectLength, WriteFlags};
use tempfile::tempdir;

const ENTRY_COUNTS: &[u32] = &[100, 1_000, 10_000, 100_000];
const VALUE_SIZES: &[usize] = &[32, 128, 512];

fn format_key(i: u32) -> String {
    format!("key{i:028}")
}

fn make_value(i: u32, size: usize) -> Vec<u8> {
    let seed = format!("data{i:010}");
    seed.as_bytes().iter().copied().cycle().take(size).collect()
}

/// Set up a plain environment (default db only) with N entries pre-populated.
fn setup_scaling_env(n: u32, value_size: usize) -> (tempfile::TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..n {
            txn.put(db, format_key(i), make_value(i, value_size), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
}

// PARITY: evmdb/sequential_get — DO NOT EDIT without updating evmdb
fn bench_sequential_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::sequential_get");

    for &size in VALUE_SIZES {
        for &n in ENTRY_COUNTS {
            let (_dir, env) = setup_scaling_env(n, size);
            let keys: Vec<String> = (0..n).map(format_key).collect();

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter(|| {
                    let txn = env.begin_ro_unsync().unwrap();
                    let db = txn.open_db(None).unwrap();
                    let mut total = 0usize;
                    for key in &keys {
                        total +=
                            *txn.get::<ObjectLength>(db.dbi(), key.as_bytes()).unwrap().unwrap();
                    }
                    total
                })
            });
        }
    }
    group.finish();
}

// PARITY: evmdb/random_get — DO NOT EDIT without updating evmdb
fn bench_random_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::random_get");

    for &size in VALUE_SIZES {
        for &n in ENTRY_COUNTS {
            let (_dir, env) = setup_scaling_env(n, size);
            let mut keys: Vec<String> = (0..n).map(format_key).collect();
            keys.shuffle(&mut StdRng::from_seed(Default::default()));

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter(|| {
                    let txn = env.begin_ro_unsync().unwrap();
                    let db = txn.open_db(None).unwrap();
                    let mut total = 0usize;
                    for key in &keys {
                        total +=
                            *txn.get::<ObjectLength>(db.dbi(), key.as_bytes()).unwrap().unwrap();
                    }
                    total
                })
            });
        }
    }
    group.finish();
}

// PARITY: evmdb/full_iteration — DO NOT EDIT without updating evmdb
fn bench_full_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::full_iteration");

    for &size in VALUE_SIZES {
        for &n in ENTRY_COUNTS {
            let (_dir, env) = setup_scaling_env(n, size);

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter(|| {
                    let txn = env.begin_ro_unsync().unwrap();
                    let db = txn.open_db(None).unwrap();
                    let mut cursor = txn.cursor(db).unwrap();
                    let mut count = 0usize;
                    while cursor.next::<Vec<u8>, Vec<u8>>().unwrap().is_some() {
                        count += 1;
                    }
                    count
                })
            });
        }
    }
    group.finish();
}

// PARITY: evmdb/put_sorted — DO NOT EDIT without updating evmdb
fn bench_append_ordered_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::append_ordered_put");

    for &size in VALUE_SIZES {
        for &n in ENTRY_COUNTS {
            let items: Vec<(String, Vec<u8>)> =
                (0..n).map(|i| (format_key(i), make_value(i, size))).collect();

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let env = Environment::builder().open(dir.path()).unwrap();
                        (dir, env)
                    },
                    |(_dir, env)| {
                        let txn = env.begin_rw_unsync().unwrap();
                        let db = txn.open_db(None).unwrap();
                        for (key, data) in &items {
                            txn.append(db, key.as_bytes(), data.as_slice()).unwrap();
                        }
                        txn.commit().unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        bench_sequential_get,
        bench_random_get,
        bench_full_iteration,
        bench_append_ordered_put,
}

criterion_main!(benches);
