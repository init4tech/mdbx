#![allow(missing_docs, dead_code)]
mod utils;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use signet_libmdbx::{Environment, ObjectLength, WriteFlags};
use std::{
    sync::{Arc, Barrier},
    thread,
};
use tempfile::tempdir;
use utils::*;

const N_ROWS: u32 = 1_000;
const READER_COUNTS: &[usize] = &[1, 4, 8];

fn setup_arc_env(n: u32) -> (tempfile::TempDir, Arc<Environment>) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..n {
            txn.put(db, get_key(i), get_data(i), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, Arc::new(env))
}

/// N readers, no writer — read throughput baseline.
fn bench_n_readers_no_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent::readers_no_writer");

    for &n_readers in READER_COUNTS {
        let (_dir, env) = setup_arc_env(N_ROWS);
        let keys: Arc<Vec<String>> = Arc::new((0..N_ROWS).map(get_key).collect());

        group.bench_with_input(
            BenchmarkId::from_parameter(n_readers),
            &n_readers,
            |b, &n_readers| {
                b.iter_batched(
                    || Arc::new(Barrier::new(n_readers + 1)),
                    |barrier| {
                        let handles: Vec<_> = (0..n_readers)
                            .map(|_| {
                                let env = Arc::clone(&env);
                                let keys = Arc::clone(&keys);
                                let barrier = Arc::clone(&barrier);
                                thread::spawn(move || {
                                    let txn = env.begin_ro_sync().unwrap();
                                    let db = txn.open_db(None).unwrap();
                                    barrier.wait();
                                    let mut total = 0usize;
                                    for key in keys.iter() {
                                        total += *txn
                                            .get::<ObjectLength>(db.dbi(), key.as_bytes())
                                            .unwrap()
                                            .unwrap();
                                    }
                                    total
                                })
                            })
                            .collect();
                        barrier.wait();
                        handles.into_iter().for_each(|h| {
                            h.join().unwrap();
                        });
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }
    group.finish();
}

/// N readers + 1 writer — read throughput under write contention.
fn bench_n_readers_one_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent::readers_one_writer");

    for &n_readers in READER_COUNTS {
        let (_dir, env) = setup_arc_env(N_ROWS);
        let keys: Arc<Vec<String>> = Arc::new((0..N_ROWS).map(get_key).collect());

        group.bench_with_input(
            BenchmarkId::from_parameter(n_readers),
            &n_readers,
            |b, &n_readers| {
                b.iter_batched(
                    || Arc::new(Barrier::new(n_readers + 2)),
                    |barrier| {
                        // Spawn readers.
                        let reader_handles: Vec<_> = (0..n_readers)
                            .map(|_| {
                                let env = Arc::clone(&env);
                                let keys = Arc::clone(&keys);
                                let barrier = Arc::clone(&barrier);
                                thread::spawn(move || {
                                    let txn = env.begin_ro_sync().unwrap();
                                    let db = txn.open_db(None).unwrap();
                                    barrier.wait();
                                    let mut total = 0usize;
                                    for key in keys.iter() {
                                        total += *txn
                                            .get::<ObjectLength>(db.dbi(), key.as_bytes())
                                            .unwrap()
                                            .unwrap();
                                    }
                                    total
                                })
                            })
                            .collect();

                        // Spawn one writer.
                        let writer = {
                            let env = Arc::clone(&env);
                            let barrier = Arc::clone(&barrier);
                            thread::spawn(move || {
                                barrier.wait();
                                let txn = env.begin_rw_sync().unwrap();
                                let db = txn.open_db(None).unwrap();
                                txn.put(db, b"writer_key", b"writer_val", WriteFlags::empty())
                                    .unwrap();
                                txn.commit().unwrap();
                            })
                        };

                        barrier.wait();
                        writer.join().unwrap();
                        reader_handles.into_iter().for_each(|h| {
                            h.join().unwrap();
                        });
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }
    group.finish();
}

/// Single-thread comparison: sync vs unsync transaction creation.
fn bench_single_thread_sync_vs_unsync(c: &mut Criterion) {
    let (_dir, env) = setup_arc_env(N_ROWS);
    let keys: Vec<String> = (0..N_ROWS).map(get_key).collect();

    c.bench_function("concurrent::single_thread::sync", |b| {
        b.iter(|| {
            let txn = env.begin_ro_sync().unwrap();
            let db = txn.open_db(None).unwrap();
            let mut total = 0usize;
            for key in &keys {
                total += *txn.get::<ObjectLength>(db.dbi(), key.as_bytes()).unwrap().unwrap();
            }
            total
        })
    });

    c.bench_function("concurrent::single_thread::unsync", |b| {
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

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        bench_n_readers_no_writer,
        bench_n_readers_one_writer,
        bench_single_thread_sync_vs_unsync,
}

criterion_main!(benches);
