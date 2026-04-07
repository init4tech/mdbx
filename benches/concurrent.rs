#![allow(missing_docs, dead_code)]
mod utils;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use signet_libmdbx::{ObjectLength, WriteFlags};
use std::{
    borrow::Cow,
    hint::black_box,
    sync::{Arc, Barrier},
    thread,
};
use utils::{bench_key, bench_value, quick_config, setup_bench_env_with_max_readers};

const N_ROWS: u32 = 1_000;
const READER_COUNTS: &[usize] = &[1, 4, 8, 32, 128];

/// Max readers set high enough for the largest reader count plus criterion
/// overhead threads.
const MAX_READERS: u64 = 256;

fn bench_n_readers_no_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent::readers_no_writer");

    for &n_readers in READER_COUNTS {
        let (_dir, env) = setup_bench_env_with_max_readers(N_ROWS, Some(MAX_READERS));
        let env = Arc::new(env);
        let keys: Arc<Vec<[u8; 32]>> = Arc::new((0..N_ROWS).map(bench_key).collect());
        // Open the db handle once — dbi is stable for the environment lifetime.
        let db = {
            let txn = env.begin_ro_sync().unwrap();
            txn.open_db(None).unwrap()
        };

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
                                    barrier.wait();
                                    let mut total = 0usize;
                                    for key in keys.iter() {
                                        let val: Cow<'_, [u8]> =
                                            txn.get(db.dbi(), key.as_slice()).unwrap().unwrap();
                                        total += val.len();
                                    }
                                    black_box(total)
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

fn bench_n_readers_one_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent::readers_one_writer");

    for &n_readers in READER_COUNTS {
        let (_dir, env) = setup_bench_env_with_max_readers(N_ROWS, Some(MAX_READERS));
        let env = Arc::new(env);
        let keys: Arc<Vec<[u8; 32]>> = Arc::new((0..N_ROWS).map(bench_key).collect());
        // Open the db handle once — dbi is stable for the environment lifetime.
        let db = {
            let txn = env.begin_ro_sync().unwrap();
            txn.open_db(None).unwrap()
        };

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
                                    barrier.wait();
                                    let mut total = 0usize;
                                    for key in keys.iter() {
                                        let val: Cow<'_, [u8]> =
                                            txn.get(db.dbi(), key.as_slice()).unwrap().unwrap();
                                        total += val.len();
                                    }
                                    black_box(total)
                                })
                            })
                            .collect();

                        // Spawn one writer inserting one extra entry.
                        let writer = {
                            let env = Arc::clone(&env);
                            let barrier = Arc::clone(&barrier);
                            thread::spawn(move || {
                                barrier.wait();
                                let txn = env.begin_rw_sync().unwrap();
                                txn.put(
                                    db,
                                    bench_key(N_ROWS + 1),
                                    bench_value(N_ROWS + 1),
                                    WriteFlags::empty(),
                                )
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
    let (_dir, env) = setup_bench_env_with_max_readers(N_ROWS, None);
    let keys: Arc<Vec<[u8; 32]>> = Arc::new((0..N_ROWS).map(bench_key).collect());

    c.bench_function("concurrent::single_thread::sync", |b| {
        b.iter(|| {
            let txn = env.begin_ro_sync().unwrap();
            let db = txn.open_db(None).unwrap();
            let mut total = 0usize;
            for key in keys.iter() {
                total += *txn.get::<ObjectLength>(db.dbi(), key.as_slice()).unwrap().unwrap();
            }
            black_box(total)
        })
    });

    c.bench_function("concurrent::single_thread::unsync", |b| {
        b.iter(|| {
            let txn = env.begin_ro_unsync().unwrap();
            let db = txn.open_db(None).unwrap();
            let mut total = 0usize;
            for key in keys.iter() {
                total += *txn.get::<ObjectLength>(db.dbi(), key.as_slice()).unwrap().unwrap();
            }
            black_box(total)
        })
    });
}

criterion_group! {
    name = benches;
    config = quick_config();
    targets =
        bench_n_readers_no_writer,
        bench_n_readers_one_writer,
        bench_single_thread_sync_vs_unsync,
}

criterion_main!(benches);
