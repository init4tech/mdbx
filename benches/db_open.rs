#![allow(missing_docs, unreachable_pub)]
mod utils;
use utils::*;

use criterion::{Criterion, criterion_group, criterion_main};
use signet_libmdbx::ffi::*;
use std::{hint::black_box, ptr};

/// Benchmark mdbx_dbi_flags_ex in isolation (on already-open DBI)
fn bench_dbi_flags_ex_only(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);

    let txn = unsafe {
        let mut txn: *mut MDBX_txn = ptr::null_mut();
        env.with_raw_env_ptr(|env_ptr| {
            txn = create_ro_raw(env_ptr);
        });
        txn
    };

    let mut dbi: MDBX_dbi = 0;
    let flags = 0;
    match unsafe { mdbx_dbi_open(txn, ptr::null(), flags, &mut dbi) } {
        MDBX_SUCCESS | MDBX_RESULT_TRUE => {}
        err => panic!("mdbx_dbi_open failed: {}", err),
    };

    c.bench_function("db_cache::ffi::flags", |b| {
        b.iter(|| {
            let mut flags: u32 = 0;
            let mut state: u32 = 0;
            black_box(unsafe { mdbx_dbi_flags_ex(txn.cast_const(), dbi, &mut flags, &mut state) });
        })
    });
}

/// Baseline: just mdbx_dbi_open without flags_ex
fn bench_dbi_open_only(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);

    let txn = unsafe {
        let mut txn: *mut MDBX_txn = ptr::null_mut();
        env.with_raw_env_ptr(|env_ptr| {
            txn = create_ro_raw(env_ptr);
        });
        txn
    };

    c.bench_function("db_cache::ffi::open", |b| {
        b.iter(|| {
            let mut dbi: MDBX_dbi = 0;
            let flags = 0;
            black_box(match unsafe { mdbx_dbi_open(txn, ptr::null(), flags, &mut dbi) } {
                MDBX_SUCCESS => false,
                MDBX_RESULT_TRUE => true,
                _ => panic!(),
            });
        })
    });
}

/// Full open path: mdbx_dbi_open + mdbx_dbi_flags_ex
fn bench_dbi_open_with_flags_ex(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);

    let txn = unsafe {
        let mut txn: *mut MDBX_txn = ptr::null_mut();
        env.with_raw_env_ptr(|env_ptr| {
            txn = create_ro_raw(env_ptr);
        });
        txn
    };

    c.bench_function("db_cache::ffi::open_plus_flags", |b| {
        b.iter(|| {
            let mut dbi: MDBX_dbi = 0;
            let flags = 0;
            black_box(match unsafe { mdbx_dbi_open(txn, ptr::null(), flags, &mut dbi) } {
                MDBX_SUCCESS => false,
                MDBX_RESULT_TRUE => true,
                _ => panic!(),
            });
            let mut flags: u32 = 0;
            let mut state: u32 = 0;
            black_box(unsafe { mdbx_dbi_flags_ex(txn.cast_const(), dbi, &mut flags, &mut state) });
        });
    });
}

/// Benchmark cached DB opens (cache hits after first call)
fn bench_open_db_cached(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);

    // Prime the cache
    let txn = env.begin_ro_unsync().unwrap();
    let _ = txn.open_db(None).unwrap();

    c.bench_function("db_cache::unnamed::hit", |b| {
        b.iter(|| black_box(txn.open_db(None).unwrap()))
    });
}

/// Benchmark uncached DB opens (always FFI call)
fn bench_open_db_no_cache(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_unsync().unwrap();

    c.bench_function("db_cache::unnamed::disabled", |b| {
        b.iter(|| black_box(txn.open_db_no_cache(None).unwrap()))
    });
}

/// Benchmark cached DB opens (cache hits after first call)
fn bench_open_db_cached_named(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_unsync().unwrap();
    // Prime the cache
    let _ = txn.open_db(Some(NAMED_DB)).unwrap();

    c.bench_function("db_cache::named::hit", |b| {
        b.iter(|| black_box(txn.open_db(Some(NAMED_DB)).unwrap()))
    });
}

/// Benchmark uncached DB opens (always FFI call)
fn bench_open_db_no_cache_named(c: &mut Criterion) {
    let (_dir, env) = setup_bench_db(10);
    let txn = env.begin_ro_unsync().unwrap();

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
