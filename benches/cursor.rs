#![allow(missing_docs)]
mod utils;

use criterion::{Criterion, criterion_group, criterion_main};
use signet_libmdbx::{Cursor, ObjectLength, ReadResult, TransactionKind, ffi::*, tx::TxPtrAccess};
use std::{hint::black_box, ptr};
use utils::*;

/// Benchmark of iterator sequential read performance.
fn bench_get_seq_iter(c: &mut Criterion) {
    let n = 100;
    let (_dir, env) = setup_bench_db(n);
    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    // Note: setup_bench_db creates a named database which adds metadata to the
    // main database, so actual item count is n + 1
    let actual_items = n + 1;
    c.bench_function("cursor::traverse::iter_x3", |b| {
        b.iter(|| {
            let mut cursor = txn.cursor(db).unwrap();
            let mut i = 0;
            let mut count = 0u32;

            for (key_len, data_len) in
                cursor.iter::<ObjectLength, ObjectLength>().map(Result::unwrap)
            {
                i = i + *key_len + *data_len;
                count += 1;
            }
            for (key_len, data_len) in
                cursor.iter::<ObjectLength, ObjectLength>().filter_map(Result::ok)
            {
                i = i + *key_len + *data_len;
                count += 1;
            }

            fn iterate<K: TransactionKind, A: TxPtrAccess>(
                cursor: &mut Cursor<K, A>,
            ) -> ReadResult<()> {
                let mut i = 0;
                for result in cursor.iter::<ObjectLength, ObjectLength>() {
                    let (key_len, data_len) = result?;
                    i = i + *key_len + *data_len;
                }
                Ok(())
            }
            iterate(&mut cursor).unwrap();

            black_box(i);
            // Both loops iterate all items since iter() repositions exhausted cursors
            assert_eq!(count, actual_items * 2);
        })
    });
}

/// Benchmark of cursor sequential read performance.
fn bench_get_seq_cursor(c: &mut Criterion) {
    let n = 100;
    let (_dir, env) = setup_bench_db(n);
    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    // Note: setup_bench_db creates a named database which adds metadata to the
    // main database, so actual item count is n + 1
    let actual_items = n + 1;
    c.bench_function("cursor::traverse::iter", |b| {
        b.iter(|| {
            let (i, count) = txn
                .cursor(db)
                .unwrap()
                .iter::<ObjectLength, ObjectLength>()
                .map(Result::unwrap)
                .fold((0, 0), |(i, count), (key, val)| (i + *key + *val, count + 1));

            black_box(i);
            assert_eq!(count, actual_items);
        })
    });
}

/// Benchmark of iterator sequential read performance (single-thread).
fn bench_get_seq_iter_single_thread(c: &mut Criterion) {
    let n = 100;
    let (_dir, env) = setup_bench_db(n);
    let mut txn = env.begin_ro_unsync().unwrap();
    let db = txn.open_db(None).unwrap();
    // Note: setup_bench_db creates a named database which adds metadata to the
    // main database, so actual item count is n + 1
    let actual_items = n + 1;
    c.bench_function("cursor::traverse::iter_x3::single_thread", |b| {
        b.iter(|| {
            let mut cursor = txn.cursor(db).unwrap();
            let mut i = 0;
            let mut count = 0u32;

            for (key_len, data_len) in
                cursor.iter::<ObjectLength, ObjectLength>().map(Result::unwrap)
            {
                i = i + *key_len + *data_len;
                count += 1;
            }
            for (key_len, data_len) in
                cursor.iter::<ObjectLength, ObjectLength>().filter_map(Result::ok)
            {
                i = i + *key_len + *data_len;
                count += 1;
            }

            fn iterate<K: TransactionKind, A: TxPtrAccess>(
                cursor: &mut Cursor<K, A>,
            ) -> ReadResult<()> {
                let mut i = 0;
                for result in cursor.iter::<ObjectLength, ObjectLength>() {
                    let (key_len, data_len) = result?;
                    i = i + *key_len + *data_len;
                }
                Ok(())
            }
            iterate(&mut cursor).unwrap();

            black_box(i);
            // Both loops iterate all items since iter() repositions exhausted cursors
            assert_eq!(count, actual_items * 2);
        })
    });
}

/// Benchmark of cursor sequential read performance (single-thread).
fn bench_get_seq_cursor_single_thread(c: &mut Criterion) {
    let n = 100;
    let (_dir, env) = setup_bench_db(n);
    let mut txn = env.begin_ro_unsync().unwrap();
    let db = txn.open_db(None).unwrap();
    // Note: setup_bench_db creates a named database which adds metadata to the
    // main database, so actual item count is n + 1
    let actual_items = n + 1;
    c.bench_function("cursor::traverse::iter::single_thread", |b| {
        b.iter(|| {
            let (i, count) = txn
                .cursor(db)
                .unwrap()
                .iter::<ObjectLength, ObjectLength>()
                .map(Result::unwrap)
                .fold((0, 0), |(i, count), (key, val)| (i + *key + *val, count + 1));

            black_box(i);
            assert_eq!(count, actual_items);
        })
    });
}

/// Benchmark of raw MDBX sequential read performance (control).
fn bench_get_seq_raw(c: &mut Criterion) {
    let n = 100;
    let (_dir, env) = setup_bench_db(n);

    let dbi = env.begin_ro_txn().unwrap().open_db(None).unwrap().dbi();
    let txn = env.begin_ro_txn().unwrap();

    let mut key = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
    let mut data = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };
    let mut cursor: *mut MDBX_cursor = ptr::null_mut();

    // Note: setup_bench_db creates a named database which adds metadata to the
    // main database, so actual item count is n + 1
    let actual_items = n + 1;

    c.bench_function("cursor::traverse::raw", |b| {
        b.iter(|| unsafe {
            txn.txn_execute(|txn| {
                mdbx_cursor_open(txn, dbi, &raw mut cursor);
                let mut i = 0;
                let mut count = 0u32;

                while mdbx_cursor_get(cursor, &raw mut key, &raw mut data, MDBX_NEXT) == 0 {
                    i += key.iov_len + data.iov_len;
                    count += 1;
                }

                black_box(i);
                assert_eq!(count, actual_items);
                mdbx_cursor_close(cursor);
            })
            .unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_get_seq_iter, bench_get_seq_cursor, bench_get_seq_raw,
              bench_get_seq_iter_single_thread, bench_get_seq_cursor_single_thread
}
criterion_main!(benches);
