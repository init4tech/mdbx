#![allow(missing_docs, unreachable_pub)]
mod utils;

use criterion::{Criterion, criterion_group, criterion_main};
use rand::{SeedableRng, prelude::SliceRandom, rngs::StdRng};
use signet_libmdbx::{ObjectLength, WriteFlags, ffi::*};
use std::{hint::black_box, ptr};
use utils::*;

// GET

fn bench_get_rand_raw(c: &mut Criterion) {
    let n = 100u32;
    let (_dir, env) = setup_bench_db(n);
    let txn = create_ro_sync(&env);
    let db = txn.open_db(None).unwrap();

    let mut keys: Vec<String> = (0..n).map(get_key).collect();
    keys.shuffle(&mut StdRng::from_seed(Default::default()));

    let dbi = db.dbi();

    let mut key_val: MDBX_val = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
    let mut data_val: MDBX_val = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

    let txn: *mut MDBX_txn = unsafe {
        let mut txn: *mut MDBX_txn = ptr::null_mut();
        env.with_raw_env_ptr(|env_ptr| {
            txn = create_ro_raw(env_ptr);
        });
        txn
    };

    c.bench_function("transaction::get::rand::raw", |b| {
        b.iter(|| unsafe {
            let mut i = 0;
            for key in &keys {
                key_val.iov_len = key.len();
                key_val.iov_base = key.as_bytes().as_ptr().cast_mut().cast();

                mdbx_get(txn, dbi, &raw const key_val, &raw mut data_val);

                i += key_val.iov_len;
            }
            black_box(i);
        })
    });
}

fn bench_get_rand_sync(c: &mut Criterion) {
    let n = 100u32;
    let (_dir, env) = setup_bench_db(n);
    let txn = create_ro_sync(&env);
    let db = txn.open_db(None).unwrap();

    let mut keys: Vec<String> = (0..n).map(get_key).collect();
    keys.shuffle(&mut StdRng::from_seed(Default::default()));

    c.bench_function("transaction::get::rand", |b| {
        b.iter(|| {
            let mut i = 0usize;
            for key in &keys {
                i += *txn.get::<ObjectLength>(db.dbi(), key.as_bytes()).unwrap().unwrap();
            }
            black_box(i);
        })
    });
}

fn bench_get_rand_unsync(c: &mut Criterion) {
    let n = 100u32;
    let (_dir, env) = setup_bench_db(n);
    let txn = create_ro_unsync(&env);
    let db = txn.open_db(None).unwrap();

    let mut keys: Vec<String> = (0..n).map(get_key).collect();
    keys.shuffle(&mut StdRng::from_seed(Default::default()));

    c.bench_function("transaction::get::rand::single_thread", |b| {
        b.iter(|| {
            let mut i = 0usize;
            for key in &keys {
                i += *txn.get::<ObjectLength>(db.dbi(), key.as_bytes()).unwrap().unwrap();
            }
            black_box(i);
        })
    });
}

// PUT

fn bench_put_rand_raw(c: &mut Criterion) {
    let n = 100u32;
    let (_dir, env) = setup_bench_db(0);

    let mut items: Vec<(String, Vec<u8>)> = (0..n).map(|n| (get_key(n), get_data(n))).collect();
    items.shuffle(&mut StdRng::from_seed(Default::default()));

    let dbi = create_ro_sync(&env).open_db(None).unwrap().dbi();

    let mut key_val: MDBX_val = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
    let mut data_val: MDBX_val = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

    c.bench_function("transaction::put::rand::raw", |b| {
        b.iter_batched(
            || unsafe {
                let mut txn: *mut MDBX_txn = ptr::null_mut();
                env.with_raw_env_ptr(|env_ptr| {
                    txn = create_rw_raw(env_ptr);
                });
                txn
            },
            |txn| unsafe {
                let mut i = 0;
                for (key, data) in &items {
                    key_val.iov_len = key.len();
                    key_val.iov_base = key.as_bytes().as_ptr().cast_mut().cast();
                    data_val.iov_len = data.len();
                    data_val.iov_base = data.as_ptr().cast_mut().cast();

                    i += mdbx_put(txn, dbi, &raw const key_val, &raw mut data_val, 0);
                }
                assert_eq!(0, i);
                mdbx_txn_abort(txn);
            },
            criterion::BatchSize::PerIteration,
        )
    });
}

fn bench_put_rand_sync(c: &mut Criterion) {
    let n = 100u32;
    let (_dir, env) = setup_bench_db(0);

    let mut items: Vec<(String, Vec<u8>)> = (0..n).map(|n| (get_key(n), get_data(n))).collect();
    items.shuffle(&mut StdRng::from_seed(Default::default()));

    c.bench_function("transaction::put::rand", |b| {
        b.iter_batched(
            || {
                let txn = create_rw_sync(&env);
                let db = txn.open_db(None).unwrap();
                (txn, db)
            },
            |(txn, db)| {
                for (key, data) in &items {
                    txn.put(db, key, data, WriteFlags::empty()).unwrap();
                }
            },
            criterion::BatchSize::PerIteration,
        )
    });
}

fn bench_put_rand_unsync(c: &mut Criterion) {
    let n = 100u32;
    let (_dir, env) = setup_bench_db(0);

    let mut items: Vec<(String, Vec<u8>)> = (0..n).map(|n| (get_key(n), get_data(n))).collect();
    items.shuffle(&mut StdRng::from_seed(Default::default()));

    c.bench_function("transaction::put::rand::single_thread", |b| {
        b.iter_batched(
            || {
                let txn = create_rw_unsync(&env);
                let db = txn.open_db(None).unwrap();
                (txn, db)
            },
            |(txn, db)| {
                for (key, data) in &items {
                    txn.put(db, key, data, WriteFlags::empty()).unwrap();
                }
            },
            criterion::BatchSize::PerIteration,
        )
    });
}

// CREATE

fn bench_tx_create_raw(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(0);

    c.bench_function("transaction::create::raw", |b| {
        b.iter(|| unsafe {
            env.with_raw_env_ptr(|env_ptr| {
                let txn = create_ro_raw(env_ptr);
                mdbx_txn_abort(txn);
            })
        })
    });
}

fn bench_tx_create_sync(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(0);

    c.bench_function("transaction::create::sync", |b| b.iter(|| black_box(create_ro_sync(&env))));
}

fn bench_tx_create_unsync(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(0);

    c.bench_function("transaction::create::unsync", |b| {
        b.iter(|| black_box(create_ro_unsync(&env)))
    });
}

// COMMIT

const COMMIT_ENTRY_COUNTS: &[u32] = &[10, 100, 1_000, 10_000];
const COMMIT_VALUE_SIZES: &[usize] = &[32, 128, 512];

fn make_commit_value(i: u32, size: usize) -> Vec<u8> {
    let seed = format!("data{i:010}");
    seed.as_bytes().iter().copied().cycle().take(size).collect()
}

/// Measures commit cost in isolation. The setup phase writes N entries of
/// a given value size (excluded from timing), then the timed phase calls
/// only `commit()`.
fn bench_commit_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction::commit");

    for &size in COMMIT_VALUE_SIZES {
        for &n in COMMIT_ENTRY_COUNTS {
            let keys: Vec<String> = (0..n).map(get_key).collect();
            let values: Vec<Vec<u8>> = (0..n).map(|i| make_commit_value(i, size)).collect();

            group.bench_with_input(
                criterion::BenchmarkId::new(format!("{size}B"), n),
                &n,
                |b, _| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().unwrap();
                            let env =
                                signet_libmdbx::Environment::builder().open(dir.path()).unwrap();
                            let txn = env.begin_rw_unsync().unwrap();
                            let db = txn.open_db(None).unwrap();
                            for (key, value) in keys.iter().zip(values.iter()) {
                                txn.put(db, key, value, WriteFlags::empty()).unwrap();
                            }
                            (dir, env, txn)
                        },
                        |(_dir, _env, txn)| {
                            txn.commit().unwrap();
                        },
                        criterion::BatchSize::PerIteration,
                    )
                },
            );
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = quick_config();
    targets = bench_get_rand_sync, bench_get_rand_raw, bench_get_rand_unsync,
              bench_put_rand_sync, bench_put_rand_raw, bench_put_rand_unsync,
              bench_tx_create_raw, bench_tx_create_sync, bench_tx_create_unsync,
              bench_commit_cost
}
criterion_main!(benches);
