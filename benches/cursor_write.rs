#![allow(missing_docs, dead_code)]
mod utils;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use signet_libmdbx::{DatabaseFlags, WriteFlags};
use utils::*;

const N: u32 = 100;
const DUPSORT_DB: &str = "dupsort_bench";

/// Set up a plain (no named sub-databases) environment with N key-value pairs.
fn setup_plain_env(n: u32) -> (tempfile::TempDir, signet_libmdbx::Environment) {
    let dir = tempfile::tempdir().unwrap();
    let env = signet_libmdbx::Environment::builder().open(dir.path()).unwrap();
    {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..n {
            txn.put(db, get_key(i), get_data(i), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
}

// PUT

// PARITY: evmdb/write_put_100
fn bench_cursor_put_sync(c: &mut Criterion) {
    let items: Vec<(String, Vec<u8>)> = (0..N).map(|i| (get_key(i), get_data(i))).collect();
    let (_dir, env) = setup_plain_env(0);

    c.bench_function("cursor_write::put::sync", |b| {
        b.iter_batched(
            || {
                let txn = create_rw_sync(&env);
                let db = txn.open_db(None).unwrap();
                (txn, db)
            },
            |(txn, db)| {
                let mut cursor = txn.cursor(db).unwrap();
                for (key, data) in &items {
                    cursor.put(key.as_bytes(), data.as_slice(), WriteFlags::empty()).unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

// PARITY: evmdb/write_put_100
fn bench_cursor_put_unsync(c: &mut Criterion) {
    let items: Vec<(String, Vec<u8>)> = (0..N).map(|i| (get_key(i), get_data(i))).collect();
    let (_dir, env) = setup_plain_env(0);

    c.bench_function("cursor_write::put::single_thread", |b| {
        b.iter_batched(
            || {
                let txn = create_rw_unsync(&env);
                let db = txn.open_db(None).unwrap();
                (txn, db)
            },
            |(txn, db)| {
                let mut cursor = txn.cursor(db).unwrap();
                for (key, data) in &items {
                    cursor.put(key.as_bytes(), data.as_slice(), WriteFlags::empty()).unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

// DEL

fn bench_cursor_del_sync(c: &mut Criterion) {
    c.bench_function("cursor_write::del::sync", |b| {
        b.iter_batched(
            || setup_plain_env(N),
            |(_dir, env)| {
                let txn = create_rw_sync(&env);
                let db = txn.open_db(None).unwrap();
                let mut cursor = txn.cursor(db).unwrap();
                cursor.first::<(), ()>().unwrap();
                while cursor.get_current::<(), ()>().unwrap().is_some() {
                    cursor.del().unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

fn bench_cursor_del_unsync(c: &mut Criterion) {
    c.bench_function("cursor_write::del::single_thread", |b| {
        b.iter_batched(
            || setup_plain_env(N),
            |(_dir, env)| {
                let txn = create_rw_unsync(&env);
                let db = txn.open_db(None).unwrap();
                let mut cursor = txn.cursor(db).unwrap();
                cursor.first::<(), ()>().unwrap();
                while cursor.get_current::<(), ()>().unwrap().is_some() {
                    cursor.del().unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

// APPEND

// PARITY: evmdb/write_put_100_sorted
fn bench_cursor_append_sync(c: &mut Criterion) {
    // Keys must be lexicographically sorted for append; zero-pad to ensure order.
    let items: Vec<(String, Vec<u8>)> = (0..N).map(|i| (get_key(i), get_data(i))).collect();
    let (_dir, env) = setup_plain_env(0);

    c.bench_function("cursor_write::append::sync", |b| {
        b.iter_batched(
            || {
                let txn = create_rw_sync(&env);
                let db = txn.open_db(None).unwrap();
                (txn, db)
            },
            |(txn, db)| {
                let mut cursor = txn.cursor(db).unwrap();
                for (key, data) in &items {
                    cursor.append(key.as_bytes(), data.as_slice()).unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

// PARITY: evmdb/write_put_100_sorted
fn bench_cursor_append_unsync(c: &mut Criterion) {
    let items: Vec<(String, Vec<u8>)> = (0..N).map(|i| (get_key(i), get_data(i))).collect();
    let (_dir, env) = setup_plain_env(0);

    c.bench_function("cursor_write::append::single_thread", |b| {
        b.iter_batched(
            || {
                let txn = create_rw_unsync(&env);
                let db = txn.open_db(None).unwrap();
                (txn, db)
            },
            |(txn, db)| {
                let mut cursor = txn.cursor(db).unwrap();
                for (key, data) in &items {
                    cursor.append(key.as_bytes(), data.as_slice()).unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

// APPEND_DUP

/// Set up a fresh environment with a DUPSORT database (no pre-existing data).
fn setup_dupsort_env() -> (tempfile::TempDir, signet_libmdbx::Environment) {
    let dir = tempfile::tempdir().unwrap();
    let env = signet_libmdbx::Environment::builder().set_max_dbs(1).open(dir.path()).unwrap();
    // Create the named DUPSORT database so it exists for subsequent transactions.
    {
        let txn = env.begin_rw_unsync().unwrap();
        txn.create_db(Some(DUPSORT_DB), DatabaseFlags::DUP_SORT).unwrap();
        txn.commit().unwrap();
    }
    (dir, env)
}

fn bench_cursor_append_dup_sync(c: &mut Criterion) {
    // One key, N duplicate values in sorted order.
    let key = b"benchkey";
    let dups: Vec<String> = (0..N).map(|i| format!("dup{i:05}")).collect();
    let (_dir, env) = setup_dupsort_env();

    c.bench_function("cursor_write::append_dup::sync", |b| {
        b.iter_batched(
            || create_rw_sync(&env),
            |txn| {
                let db = txn.open_db(Some(DUPSORT_DB)).unwrap();
                let mut cursor = txn.cursor(db).unwrap();
                for dup in &dups {
                    cursor.append_dup(key, dup.as_bytes()).unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

fn bench_cursor_append_dup_unsync(c: &mut Criterion) {
    let key = b"benchkey";
    let dups: Vec<String> = (0..N).map(|i| format!("dup{i:05}")).collect();
    let (_dir, env) = setup_dupsort_env();

    c.bench_function("cursor_write::append_dup::single_thread", |b| {
        b.iter_batched(
            || create_rw_unsync(&env),
            |txn| {
                let db = txn.open_db(Some(DUPSORT_DB)).unwrap();
                let mut cursor = txn.cursor(db).unwrap();
                for dup in &dups {
                    cursor.append_dup(key, dup.as_bytes()).unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        bench_cursor_put_sync, bench_cursor_put_unsync,
        bench_cursor_del_sync, bench_cursor_del_unsync,
        bench_cursor_append_sync, bench_cursor_append_unsync,
        bench_cursor_append_dup_sync, bench_cursor_append_dup_unsync,
}

criterion_main!(benches);
