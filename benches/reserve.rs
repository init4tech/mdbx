#![allow(missing_docs, dead_code)]
mod utils;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use signet_libmdbx::WriteFlags;
use utils::*;

const VALUE_SIZES: &[usize] = &[64, 256, 1024, 4096];
const KEY: &[u8] = b"benchkey";

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("reserve::put");
    for &size in VALUE_SIZES {
        let data = vec![0u8; size];
        let (_dir, env) = setup_bench_db(0);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || {
                    let txn = create_rw_unsync(&env);
                    let db = txn.open_db(None).unwrap();
                    (txn, db)
                },
                |(txn, db)| {
                    txn.put(db, KEY, data.as_slice(), WriteFlags::empty()).unwrap();
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

fn bench_with_reservation(c: &mut Criterion) {
    let mut group = c.benchmark_group("reserve::with_reservation");
    for &size in VALUE_SIZES {
        let data = vec![0u8; size];
        let (_dir, env) = setup_bench_db(0);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter_batched(
                || {
                    let txn = create_rw_unsync(&env);
                    let db = txn.open_db(None).unwrap();
                    (txn, db)
                },
                |(txn, db)| {
                    txn.with_reservation(db, KEY, size, WriteFlags::empty(), |buf| {
                        buf.copy_from_slice(&data);
                    })
                    .unwrap();
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
    targets = bench_put, bench_with_reservation,
}

criterion_main!(benches);
