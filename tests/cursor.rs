#![allow(missing_docs)]
use signet_libmdbx::*;
use std::{borrow::Cow, hint::black_box};
use tempfile::tempdir;

/// Convenience
type Result<T> = ReadResult<T>;

#[test]
fn test_get() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.open_db(None).unwrap();

    assert_eq!(None, txn.cursor(db).unwrap().first::<(), ()>().unwrap());

    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key2", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key3", b"val3", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.get_current().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.next().unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.prev().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.last().unwrap(), Some((*b"key3", *b"val3")));
    assert_eq!(cursor.set(b"key1").unwrap(), Some(*b"val1"));
    assert_eq!(cursor.set_key(b"key3").unwrap(), Some((*b"key3", *b"val3")));
    assert_eq!(cursor.set_range(b"key2\0").unwrap(), Some((*b"key3", *b"val3")));
}

#[test]
fn test_get_dup() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key1", b"val3", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key2", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key2", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key2", b"val3", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.first_dup().unwrap(), Some(*b"val1"));
    assert_eq!(cursor.get_current().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.next_nodup().unwrap(), Some((*b"key2", *b"val1")));
    assert_eq!(cursor.next().unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.prev().unwrap(), Some((*b"key2", *b"val1")));
    assert_eq!(cursor.next_dup().unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.next_dup().unwrap(), Some((*b"key2", *b"val3")));
    assert_eq!(cursor.next_dup::<(), ()>().unwrap(), None);
    assert_eq!(cursor.prev_dup().unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.last_dup().unwrap(), Some(*b"val3"));
    assert_eq!(cursor.prev_nodup().unwrap(), Some((*b"key1", *b"val3")));
    assert_eq!(cursor.next_dup::<(), ()>().unwrap(), None);
    assert_eq!(cursor.set(b"key1").unwrap(), Some(*b"val1"));
    assert_eq!(cursor.set(b"key2").unwrap(), Some(*b"val1"));
    assert_eq!(cursor.set_range(b"key1\0").unwrap(), Some((*b"key2", *b"val1")));
    assert_eq!(cursor.get_both(b"key1", b"val3").unwrap(), Some(*b"val3"));
    assert_eq!(cursor.get_both_range::<()>(b"key1", b"val4").unwrap(), None);
    assert_eq!(cursor.get_both_range(b"key2", b"val").unwrap(), Some(*b"val1"));

    assert_eq!(cursor.last().unwrap(), Some((*b"key2", *b"val3")));
    cursor.del(WriteFlags::empty()).unwrap();
    assert_eq!(cursor.last().unwrap(), Some((*b"key2", *b"val2")));
    cursor.del(WriteFlags::empty()).unwrap();
    assert_eq!(cursor.last().unwrap(), Some((*b"key2", *b"val1")));
    cursor.del(WriteFlags::empty()).unwrap();
    assert_eq!(cursor.last().unwrap(), Some((*b"key1", *b"val3")));
}

#[test]
fn test_get_dupfixed() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key1", b"val3", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key2", b"val4", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key2", b"val5", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key2", b"val6", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.get_multiple().unwrap(), Some(*b"val1val2val3"));
    assert_eq!(cursor.next_multiple::<(), ()>().unwrap(), None);
}

#[test]
fn test_iter() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let items: Vec<(_, _)> = vec![
        (*b"key1", *b"val1"),
        (*b"key2", *b"val2"),
        (*b"key3", *b"val3"),
        (*b"key5", *b"val5"),
    ];

    {
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();
        for (key, data) in &items {
            txn.put(db.dbi(), key, data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    // Because Result implements FromIterator, we can collect the iterator
    // of items of type Result<_, E> into a Result<Vec<_, E>> by specifying
    // the collection type via the turbofish syntax.
    assert_eq!(items, cursor.iter().collect::<Result<Vec<_>>>().unwrap());

    // Alternately, we can collect it into an appropriately typed variable.
    let retr: Result<Vec<_>> = cursor.iter_start().unwrap().collect();
    assert_eq!(items, retr.unwrap());

    cursor.set::<()>(b"key2").unwrap();
    assert_eq!(
        items.clone().into_iter().skip(2).collect::<Vec<_>>(),
        cursor.iter().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(items, cursor.iter_start().unwrap().collect::<Result<Vec<_>>>().unwrap());

    assert_eq!(
        items.clone().into_iter().skip(1).collect::<Vec<_>>(),
        cursor.iter_from(b"key2").unwrap().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(
        items.into_iter().skip(3).collect::<Vec<_>>(),
        cursor.iter_from(b"key4").unwrap().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(
        Vec::<((), ())>::new(),
        cursor.iter_from(b"key6").unwrap().collect::<Result<Vec<_>>>().unwrap()
    );
}

#[test]
fn test_iter_empty_database() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    assert!(cursor.iter::<(), ()>().next().is_none());
    assert!(cursor.iter_start::<(), ()>().unwrap().next().is_none());
    assert!(cursor.iter_from::<(), ()>(b"foo").unwrap().next().is_none());
}

#[test]
fn test_iter_empty_dup_database() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.commit().unwrap();

    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    assert!(cursor.iter::<(), ()>().next().is_none());
    assert!(cursor.iter_start::<(), ()>().unwrap().next().is_none());
    assert!(cursor.iter_from::<(), ()>(b"foo").unwrap().next().is_none());
    assert!(cursor.iter_from::<(), ()>(b"foo").unwrap().next().is_none());
    assert!(cursor.iter_dup::<(), ()>().flatten().flatten().next().is_none());
    assert!(cursor.iter_dup_start::<(), ()>().unwrap().flatten().flatten().next().is_none());
    assert!(cursor.iter_dup_from::<(), ()>(b"foo").unwrap().flatten().flatten().next().is_none());
    assert!(cursor.iter_dup_of::<(), ()>(b"foo").unwrap().next().is_none());
}

#[test]
fn test_iter_dup() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.commit().unwrap();

    let items: Vec<(_, _)> = [
        (b"a", b"1"),
        (b"a", b"2"),
        (b"a", b"3"),
        (b"b", b"1"),
        (b"b", b"2"),
        (b"b", b"3"),
        (b"c", b"1"),
        (b"c", b"2"),
        (b"c", b"3"),
        (b"e", b"1"),
        (b"e", b"2"),
        (b"e", b"3"),
    ]
    .iter()
    .map(|&(&k, &v)| (k, v))
    .collect();

    {
        let txn = env.begin_rw_txn().unwrap();
        for (key, data) in items.clone() {
            let db = txn.open_db(None).unwrap();
            txn.put(db.dbi(), key, data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();
    assert_eq!(items, cursor.iter_dup().flatten().flatten().collect::<Result<Vec<_>>>().unwrap());

    cursor.set::<()>(b"b").unwrap();
    assert_eq!(
        items.iter().copied().skip(6).collect::<Vec<_>>(),
        cursor.iter_dup().flatten().flatten().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(
        items,
        cursor.iter_dup_start().unwrap().flatten().flatten().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .iter_dup_from(b"b")
            .unwrap()
            .flatten()
            .flatten()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    );

    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .iter_dup_from(b"ab")
            .unwrap()
            .flatten()
            .flatten()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    );

    assert_eq!(
        items.iter().copied().skip(9).collect::<Vec<_>>(),
        cursor
            .iter_dup_from(b"d")
            .unwrap()
            .flatten()
            .flatten()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    );

    assert_eq!(
        Vec::<([u8; 1], [u8; 1])>::new(),
        cursor
            .iter_dup_from(b"f")
            .unwrap()
            .flatten()
            .flatten()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    );

    assert_eq!(
        items.iter().copied().skip(3).take(3).collect::<Vec<_>>(),
        cursor.iter_dup_of(b"b").unwrap().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(0, cursor.iter_dup_of::<(), ()>(b"foo").unwrap().count());
}

#[test]
fn test_iter_del_get() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let items = vec![(*b"a", *b"1"), (*b"b", *b"2")];
    {
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
        assert_eq!(
            txn.cursor(db)
                .unwrap()
                .iter_dup_of::<(), ()>(b"a")
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .len(),
            0
        );
        txn.commit().unwrap();
    }

    {
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();
        for (key, data) in &items {
            txn.put(db.dbi(), key, data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();
    assert_eq!(
        items,
        cursor.iter_dup_start().unwrap().flatten().flatten().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(
        items.iter().copied().take(1).collect::<Vec<(_, _)>>(),
        cursor.iter_dup_of(b"a").unwrap().collect::<Result<Vec<_>>>().unwrap()
    );

    assert_eq!(cursor.set(b"a").unwrap(), Some(*b"1"));

    cursor.del(WriteFlags::empty()).unwrap();

    assert_eq!(
        cursor
            .iter_dup_of::<[u8; 1], [u8; 1]>(b"a")
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn test_put_del() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    cursor.put(b"key1", b"val1", WriteFlags::empty()).unwrap();
    cursor.put(b"key2", b"val2", WriteFlags::empty()).unwrap();
    cursor.put(b"key3", b"val3", WriteFlags::empty()).unwrap();

    assert_eq!(
        cursor.set_key(b"key2").unwrap(),
        Some((Cow::Borrowed(b"key2" as &[u8]), Cow::Borrowed(b"val2" as &[u8])))
    );
    assert_eq!(
        cursor.get_current().unwrap(),
        Some((Cow::Borrowed(b"key2" as &[u8]), Cow::Borrowed(b"val2" as &[u8])))
    );

    cursor.del(WriteFlags::empty()).unwrap();
    assert_eq!(
        cursor.get_current().unwrap(),
        Some((Cow::Borrowed(b"key3" as &[u8]), Cow::Borrowed(b"val3" as &[u8])))
    );
    assert_eq!(
        cursor.last().unwrap(),
        Some((Cow::Borrowed(b"key3" as &[u8]), Cow::Borrowed(b"val3" as &[u8])))
    );
}

#[test]
fn test_dup_sort_validation_on_non_dupsort_db() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.open_db(None).unwrap(); // Non-DUPSORT database
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    cursor.first::<(), ()>().unwrap(); // Position cursor

    // These should return RequiresDupSort error
    let err = cursor.first_dup::<()>().unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupSort)));

    let err = cursor.last_dup::<()>().unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupSort)));

    let err = cursor.next_dup::<(), ()>().unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupSort)));

    let err = cursor.prev_dup::<(), ()>().unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupSort)));

    let err = cursor.get_both::<()>(b"key1", b"val1").unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupSort)));

    let err = cursor.get_both_range::<()>(b"key1", b"val").unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupSort)));
}

#[test]
fn test_dup_fixed_validation_on_non_dupfixed_db() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    // Create DUPSORT but NOT DUPFIXED database
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    cursor.first::<(), ()>().unwrap(); // Position cursor

    // These should return RequiresDupFixed error
    let err = cursor.get_multiple::<()>().unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupFixed)));

    let err = cursor.next_multiple::<(), ()>().unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupFixed)));

    let err = cursor.prev_multiple::<(), ()>().unwrap_err();
    assert!(matches!(err, ReadError::Mdbx(MdbxError::RequiresDupFixed)));
}

#[test]
fn test_dup_sort_methods_work_on_dupsort_db() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    cursor.first::<(), ()>().unwrap();

    // These should work without error on a DUPSORT database
    cursor.first_dup::<()>().unwrap();
    cursor.last_dup::<()>().unwrap();
    cursor.next_dup::<(), ()>().unwrap();
    cursor.prev_dup::<(), ()>().unwrap();
    cursor.get_both::<()>(b"key1", b"val1").unwrap();
    cursor.get_both_range::<()>(b"key1", b"val").unwrap();
}

#[test]
fn test_dup_fixed_methods_work_on_dupfixed_db() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    cursor.first::<(), ()>().unwrap();

    // These should work without error on a DUPFIXED database
    cursor.get_multiple::<()>().unwrap();
    // next_multiple and prev_multiple may return None but shouldn't error
    cursor.next_multiple::<(), ()>().unwrap();
    cursor.prev_multiple::<(), ()>().unwrap();
}

#[test]
fn test_iter_exhausted_cursor_repositions() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    for i in 0u8..100 {
        txn.put(db.dbi(), [i], [i], WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    // Loop 1: iterate through all items
    let count1 = cursor.iter::<[u8; 1], [u8; 1]>().count();
    assert_eq!(count1, 100);

    // After exhaustion, is_eof should be true
    assert!(cursor.is_eof());

    // Loop 2: iter() should reposition and iterate all items again
    let count2 = cursor.iter::<[u8; 1], [u8; 1]>().count();
    assert_eq!(count2, 100);

    // Total count should be 200
    assert_eq!(count1 + count2, 200);
}

#[test]
fn test_iter_benchmark_pattern() {
    // This test mirrors the exact logic of bench_get_seq_iter
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let n = 100u32;

    let txn = env.begin_rw_txn().unwrap();
    let db = txn.open_db(None).unwrap();
    for i in 0..n {
        let key = format!("key{i}");
        let data = format!("data{i}");
        txn.put(db.dbi(), key.as_bytes(), data.as_bytes(), WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    // Setup like benchmark: transaction and db outside the "iteration"
    let txn = env.begin_ro_txn().unwrap();
    let db = txn.open_db(None).unwrap();

    // Run the benchmark closure multiple times to match criterion behavior
    for _ in 0..3 {
        let mut cursor = txn.cursor(db).unwrap();
        let mut count = 0u32;

        // Loop 1: iterate with map(Result::unwrap), using ObjectLength like benchmark
        for (key_len, data_len) in cursor.iter::<ObjectLength, ObjectLength>().map(Result::unwrap) {
            black_box(*key_len + *data_len);
            count += 1;
        }

        // Loop 2: iterate with filter_map(Result::ok)
        for (key_len, data_len) in
            cursor.iter::<ObjectLength, ObjectLength>().filter_map(Result::ok)
        {
            black_box(*key_len + *data_len);
            count += 1;
        }

        // Loop 3: internal iterate function (doesn't affect count)
        fn iterate(cursor: &mut Cursor<signet_libmdbx::RO>) -> ReadResult<()> {
            for result in cursor.iter::<ObjectLength, ObjectLength>() {
                let (key_len, data_len) = result?;
                black_box(*key_len + *data_len);
            }
            Ok(())
        }
        iterate(&mut cursor).unwrap();

        // With the fix, both loops should iterate all items
        assert_eq!(count, n * 2);
    }
}
