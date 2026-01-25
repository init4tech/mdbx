#![allow(missing_docs, unreachable_pub)]
mod utils;
use utils::*;

use criterion::{Criterion, criterion_group, criterion_main};
use signet_libmdbx::ffi::*;
use std::{hint::black_box, ptr};

/// Benchmark mdbx_dbi_flags_ex in isolation (on already-open DBI)
fn bench_dbi_flags_ex_only(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let dbi = db.dbi();

    c.bench_function("db_cache::ffi::flags", |b| {
        b.iter(|| {
            txn.txn_execute(|txn_ptr| unsafe {
                let mut flags: u32 = 0;
                let mut state: u32 = 0;
                black_box(mdbx_dbi_flags_ex(txn_ptr.cast_const(), dbi, &mut flags, &mut state));
            })
            .unwrap();
        })
    });
}

/// Baseline: just mdbx_dbi_open without flags_ex
fn bench_dbi_open_only(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_txn().unwrap();

    c.bench_function("db_cache::ffi::open", |b| {
        b.iter(|| {
            txn.txn_execute(|txn_ptr| unsafe {
                let mut dbi: MDBX_dbi = 0;
                black_box(mdbx_dbi_open(txn_ptr, ptr::null(), 0, &mut dbi));
            })
            .unwrap();
        })
    });
}

/// Full open path: mdbx_dbi_open + mdbx_dbi_flags_ex
fn bench_dbi_open_with_flags_ex(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_txn().unwrap();

    c.bench_function("db_cache::ffi::open_plus_flags", |b| {
        b.iter(|| {
            txn.txn_execute(|txn_ptr| unsafe {
                let mut dbi: MDBX_dbi = 0;
                mdbx_dbi_open(txn_ptr, ptr::null(), 0, &mut dbi);
                let mut flags: u32 = 0;
                let mut state: u32 = 0;
                black_box(mdbx_dbi_flags_ex(txn_ptr.cast_const(), dbi, &mut flags, &mut state));
            })
            .unwrap();
        })
    });
}

/// Benchmark cached DB opens (cache hits after first call)
fn bench_open_db_cached(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_txn().unwrap();
    // Prime the cache
    let _ = txn.open_db(None).unwrap();

    c.bench_function("db_cache::unnamed::hit", |b| {
        b.iter(|| black_box(txn.open_db(None).unwrap()))
    });
}

/// Benchmark uncached DB opens (always FFI call)
fn bench_open_db_no_cache(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_txn().unwrap();

    c.bench_function("db_cache::unnamed::disabled", |b| {
        b.iter(|| black_box(txn.open_db_no_cache(None).unwrap()))
    });
}

/// Benchmark cached DB opens (cache hits after first call)
fn bench_open_db_cached_named(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_txn().unwrap();
    // Prime the cache
    let _ = txn.open_db(Some(NAMED_DB)).unwrap();

    c.bench_function("db_cache::named::hit", |b| {
        b.iter(|| black_box(txn.open_db(Some(NAMED_DB)).unwrap()))
    });
}

/// Benchmark uncached DB opens (always FFI call)
fn bench_open_db_no_cache_named(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_txn().unwrap();

    c.bench_function("db_cache::named::disabled", |b| {
        b.iter(|| black_box(txn.open_db_no_cache(Some(NAMED_DB)).unwrap()))
    });
}

criterion_group! {
    name = db_open;
    config = Criterion::default();
    targets =
        bench_dbi_flags_ex_only,
        bench_dbi_open_only,
        bench_dbi_open_with_flags_ex,
        bench_open_db_cached,
        bench_open_db_no_cache,
        bench_open_db_cached_named,
        bench_open_db_no_cache_named
}
criterion_main!(db_open);
