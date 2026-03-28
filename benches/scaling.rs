#![allow(missing_docs, dead_code)]

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{SeedableRng, prelude::SliceRandom, rngs::StdRng};
use signet_libmdbx::{Environment, ObjectLength, WriteFlags};
use tempfile::tempdir;

const ENTRY_COUNTS: &[u32] = &[100, 1_000, 10_000, 100_000];

fn format_key(i: u32) -> String {
    format!("key{i:010}")
}

fn format_data(i: u32) -> String {
    format!("data{i:010}")
}

/// Set up a plain environment (default db only) with N entries pre-populated.
fn setup_scaling_env(n: u32) -> (tempfile::TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..n {
            txn.put(db, format_key(i), format_data(i), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
}

/// Sequential get: read every entry in insertion order.
fn bench_sequential_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::sequential_get");

    for &n in ENTRY_COUNTS {
        let (_dir, env) = setup_scaling_env(n);
        let keys: Vec<String> = (0..n).map(format_key).collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = env.begin_ro_unsync().unwrap();
                let db = txn.open_db(None).unwrap();
                let mut total = 0usize;
                for key in &keys {
                    total += *txn.get::<ObjectLength>(db.dbi(), key.as_bytes()).unwrap().unwrap();
                }
                total
            })
        });
    }
    group.finish();
}

/// Random get: read every entry in shuffled order.
fn bench_random_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::random_get");

    for &n in ENTRY_COUNTS {
        let (_dir, env) = setup_scaling_env(n);
        let mut keys: Vec<String> = (0..n).map(format_key).collect();
        keys.shuffle(&mut StdRng::from_seed(Default::default()));

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let txn = env.begin_ro_unsync().unwrap();
                let db = txn.open_db(None).unwrap();
                let mut total = 0usize;
                for key in &keys {
                    total += *txn.get::<ObjectLength>(db.dbi(), key.as_bytes()).unwrap().unwrap();
                }
                total
            })
        });
    }
    group.finish();
}

/// Full iteration: walk every entry via a cursor.
fn bench_full_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::full_iteration");

    for &n in ENTRY_COUNTS {
        let (_dir, env) = setup_scaling_env(n);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
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
    group.finish();
}

/// Append-ordered put: insert N entries in key order into a fresh environment.
fn bench_append_ordered_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::append_ordered_put");

    for &n in ENTRY_COUNTS {
        // Keys use zero-padded format to ensure lexicographic ordering.
        let items: Vec<(String, String)> =
            (0..n).map(|i| (format_key(i), format_data(i))).collect();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
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
                        txn.append(db, key.as_bytes(), data.as_bytes()).unwrap();
                    }
                    txn.commit().unwrap();
                },
                BatchSize::PerIteration,
            )
        });
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
