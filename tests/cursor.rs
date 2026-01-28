#![allow(missing_docs)]
mod common;
use common::{TestRoTxn, TestRwTxn, V1Factory, V2Factory};
use signet_libmdbx::{
    Cursor, DatabaseFlags, Environment, MdbxError, MdbxResult, ObjectLength, ReadResult,
    TransactionKind, WriteFlags,
};
use std::{borrow::Cow, hint::black_box};
use tempfile::tempdir;

/// Convenience
type Result<T> = ReadResult<T>;

// =============================================================================
// Dual-variant tests (run for both V1 and V2)
// =============================================================================

fn test_get_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();

    assert_eq!(None, txn.cursor(db).unwrap().first::<(), ()>().unwrap());

    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key3", b"val3", WriteFlags::empty()).unwrap();

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
fn test_get_v1() {
    test_get_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_get_v2() {
    test_get_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_get_dup_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", b"val3", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val3", WriteFlags::empty()).unwrap();

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
fn test_get_dup_v1() {
    test_get_dup_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_get_dup_v2() {
    test_get_dup_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_get_dupfixed_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", b"val3", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val4", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val5", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val6", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.get_multiple().unwrap(), Some(*b"val1val2val3"));
    assert_eq!(cursor.next_multiple::<(), ()>().unwrap(), None);
}

#[test]
fn test_get_dupfixed_v1() {
    test_get_dupfixed_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_get_dupfixed_v2() {
    test_get_dupfixed_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let items: Vec<(_, _)> = vec![
        (*b"key1", *b"val1"),
        (*b"key2", *b"val2"),
        (*b"key3", *b"val3"),
        (*b"key5", *b"val5"),
    ];

    {
        let txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        for (key, data) in &items {
            txn.put(db, key, data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let txn = begin_ro(&env).unwrap();
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
fn test_iter_v1() {
    test_iter_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_v2() {
    test_iter_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_empty_database_impl<RwTx, RoTx>(
    _begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    assert!(cursor.iter::<(), ()>().next().is_none());
    assert!(cursor.iter_start::<(), ()>().unwrap().next().is_none());
    assert!(cursor.iter_from::<(), ()>(b"foo").unwrap().next().is_none());
}

#[test]
fn test_iter_empty_database_v1() {
    test_iter_empty_database_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_empty_database_v2() {
    test_iter_empty_database_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_empty_dup_database_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.commit().unwrap();

    let txn = begin_ro(&env).unwrap();
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
fn test_iter_empty_dup_database_v1() {
    test_iter_empty_dup_database_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_empty_dup_database_v2() {
    test_iter_empty_dup_database_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_dup_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
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
        let txn = begin_rw(&env).unwrap();
        for (key, data) in items.clone() {
            let db = txn.open_db(None).unwrap();
            txn.put(db, &key, &data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let txn = begin_ro(&env).unwrap();
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
fn test_iter_dup_v1() {
    test_iter_dup_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dup_v2() {
    test_iter_dup_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_del_get_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let items = vec![(*b"a", *b"1"), (*b"b", *b"2")];
    {
        let txn = begin_rw(&env).unwrap();
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
        let txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        for (key, data) in &items {
            txn.put(db, key, data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let txn = begin_rw(&env).unwrap();
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
fn test_iter_del_get_v1() {
    test_iter_del_get_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_del_get_v2() {
    test_iter_del_get_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_put_del_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
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
fn test_put_del_v1() {
    test_put_del_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_put_del_v2() {
    test_put_del_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

// NOTE: DUP_SORT and DUP_FIXED validation tests have been moved to the debug-only
// module below. In debug builds, these validations panic via debug_assert!
// In release builds, the checks are skipped and MDBX will return errors.

fn test_dup_sort_methods_work_on_dupsort_db_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();

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
fn test_dup_sort_methods_work_on_dupsort_db_v1() {
    test_dup_sort_methods_work_on_dupsort_db_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_dup_sort_methods_work_on_dupsort_db_v2() {
    test_dup_sort_methods_work_on_dupsort_db_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_dup_fixed_methods_work_on_dupfixed_db_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();

    let mut cursor = txn.cursor(db).unwrap();
    cursor.first::<(), ()>().unwrap();

    // These should work without error on a DUPFIXED database
    cursor.get_multiple::<()>().unwrap();
    // next_multiple and prev_multiple may return None but shouldn't error
    cursor.next_multiple::<(), ()>().unwrap();
    cursor.prev_multiple::<(), ()>().unwrap();
}

#[test]
fn test_dup_fixed_methods_work_on_dupfixed_db_v1() {
    test_dup_fixed_methods_work_on_dupfixed_db_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_dup_fixed_methods_work_on_dupfixed_db_v2() {
    test_dup_fixed_methods_work_on_dupfixed_db_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_exhausted_cursor_repositions_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    for i in 0u8..100 {
        txn.put(db, &[i], &[i], WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    let txn = begin_ro(&env).unwrap();
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
fn test_iter_exhausted_cursor_repositions_v1() {
    test_iter_exhausted_cursor_repositions_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_exhausted_cursor_repositions_v2() {
    test_iter_exhausted_cursor_repositions_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_benchmark_pattern_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    // This test mirrors the exact logic of bench_get_seq_iter
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let n = 100u32;

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    for i in 0..n {
        let key = format!("key{i}");
        let data = format!("data{i}");
        txn.put(db, key.as_bytes(), data.as_bytes(), WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    // Setup like benchmark: transaction and db outside the "iteration"
    let txn = begin_ro(&env).unwrap();
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
        fn iterate<K: TransactionKind>(cursor: &mut Cursor<K>) -> ReadResult<()> {
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

#[test]
fn test_iter_benchmark_pattern_v1() {
    test_iter_benchmark_pattern_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_benchmark_pattern_v2() {
    test_iter_benchmark_pattern_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

// =============================================================================
// Append API Tests
// =============================================================================

fn test_cursor_append_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Append keys in sorted order: a, b, c
    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    cursor.append(b"a", b"val_a").unwrap();
    cursor.append(b"b", b"val_b").unwrap();
    cursor.append(b"c", b"val_c").unwrap();

    drop(cursor);
    txn.commit().unwrap();

    // Verify data was written correctly
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    assert_eq!(cursor.first().unwrap(), Some((*b"a", *b"val_a")));
    assert_eq!(cursor.next().unwrap(), Some((*b"b", *b"val_b")));
    assert_eq!(cursor.next().unwrap(), Some((*b"c", *b"val_c")));
    assert_eq!(cursor.next::<(), ()>().unwrap(), None);
}

#[test]
fn test_cursor_append_v1() {
    test_cursor_append_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_cursor_append_v2() {
    test_cursor_append_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_tx_append_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Write using transaction-level append
    {
        let txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();

        txn.append(db, b"key1", b"val1").unwrap();
        txn.append(db, b"key2", b"val2").unwrap();
        txn.append(db, b"key3", b"val3").unwrap();

        txn.commit().unwrap();
    }

    // Verify data was written correctly
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.next().unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.next().unwrap(), Some((*b"key3", *b"val3")));
}

#[test]
fn test_tx_append_v1() {
    test_tx_append_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_tx_append_v2() {
    test_tx_append_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_append_dup_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create DUPSORT database and append duplicates
    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    // Append duplicates for key "a" in sorted order
    cursor.append_dup(b"a", b"1").unwrap();
    cursor.append_dup(b"a", b"2").unwrap();
    cursor.append_dup(b"a", b"3").unwrap();

    drop(cursor);
    txn.commit().unwrap();

    // Verify
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    assert_eq!(cursor.first().unwrap(), Some((*b"a", *b"1")));
    assert_eq!(cursor.next_dup().unwrap(), Some((*b"a", *b"2")));
    assert_eq!(cursor.next_dup().unwrap(), Some((*b"a", *b"3")));
    assert_eq!(cursor.next_dup::<(), ()>().unwrap(), None);
}

#[test]
fn test_append_dup_v1() {
    test_append_dup_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_append_dup_v2() {
    test_append_dup_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

// NOTE: append_dup DUP_SORT validation tests have been moved to the debug-only
// module below. In debug builds, these validations panic via debug_assert!
// In release builds, the checks are skipped and MDBX will return errors.

// =============================================================================
// DUPFIXED Iterator Tests
// =============================================================================

fn test_iter_dupfixed_basic_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create DUPFIXED database with 4-byte values
    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

    // Insert values for key1
    txn.put(db, b"key1", &1u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", &2u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", &3u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    // Insert values for key2
    txn.put(db, b"key2", &10u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", &20u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    txn.commit().unwrap();

    // Read back using the iterator
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    let results: Vec<(Vec<u8>, [u8; 4])> =
        cursor.iter_dupfixed_start::<Vec<u8>, 4>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0], (b"key1".to_vec(), 1u32.to_le_bytes()));
    assert_eq!(results[1], (b"key1".to_vec(), 2u32.to_le_bytes()));
    assert_eq!(results[2], (b"key1".to_vec(), 3u32.to_le_bytes()));
    assert_eq!(results[3], (b"key2".to_vec(), 10u32.to_le_bytes()));
    assert_eq!(results[4], (b"key2".to_vec(), 20u32.to_le_bytes()));
}

#[test]
fn test_iter_dupfixed_basic_v1() {
    test_iter_dupfixed_basic_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_basic_v2() {
    test_iter_dupfixed_basic_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_dupfixed_from_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create DUPFIXED database with 4-byte values
    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

    txn.put(db, b"aaa", &1u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"bbb", &2u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"ccc", &3u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"ddd", &4u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    txn.commit().unwrap();

    // Start from "bbb"
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    let results: Vec<(Vec<u8>, [u8; 4])> =
        cursor.iter_dupfixed_from::<Vec<u8>, 4>(b"bbb").unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (b"bbb".to_vec(), 2u32.to_le_bytes()));
    assert_eq!(results[1], (b"ccc".to_vec(), 3u32.to_le_bytes()));
    assert_eq!(results[2], (b"ddd".to_vec(), 4u32.to_le_bytes()));
}

#[test]
fn test_iter_dupfixed_from_v1() {
    test_iter_dupfixed_from_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_from_v2() {
    test_iter_dupfixed_from_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_dupfixed_empty_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create empty DUPFIXED database
    let txn = begin_rw(&env).unwrap();
    txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.commit().unwrap();

    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    let results: Vec<(Vec<u8>, [u8; 4])> =
        cursor.iter_dupfixed_start::<Vec<u8>, 4>().unwrap().map(|r| r.unwrap()).collect();

    assert!(results.is_empty());
}

#[test]
fn test_iter_dupfixed_empty_v1() {
    test_iter_dupfixed_empty_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_empty_v2() {
    test_iter_dupfixed_empty_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

// NOTE: iter_dupfixed DUP_FIXED validation tests have been moved to the debug-only
// module below. In debug builds, these validations panic via debug_assert!
// In release builds, the checks are skipped and MDBX will return errors.

fn test_iter_dupfixed_many_values_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    use std::collections::HashSet;

    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create DUPFIXED database with many values to test page boundaries
    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

    // Insert 1000 values to ensure we span multiple pages
    // Note: MDBX sorts duplicates lexicographically by bytes, not as integers.
    // So u32 values will be sorted by their byte representation, not numerically.
    for i in 0u32..1000 {
        txn.put(db, b"key", &i.to_le_bytes(), WriteFlags::empty()).unwrap();
    }

    txn.commit().unwrap();

    // Read back and verify all values are present (order may differ from insertion)
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    let results: Vec<(Vec<u8>, [u8; 4])> =
        cursor.iter_dupfixed_start::<Vec<u8>, 4>().unwrap().map(|r| r.unwrap()).collect();

    // Verify count
    assert_eq!(results.len(), 1000);

    // Verify all keys are "key"
    for (key, _) in &results {
        assert_eq!(key, b"key");
    }

    // Verify all 1000 values are present (regardless of order)
    let values: HashSet<u32> = results.iter().map(|(_, v)| u32::from_le_bytes(*v)).collect();
    let expected: HashSet<u32> = (0u32..1000).collect();
    assert_eq!(values, expected);

    // Verify values are in lexicographic byte order (MDBX sorts duplicates this way)
    for window in results.windows(2) {
        let v1 = &window[0].1;
        let v2 = &window[1].1;
        assert!(v1 <= v2, "values not in sorted order: {:?} > {:?}", v1, v2);
    }
}

#[test]
fn test_iter_dupfixed_many_values_v1() {
    test_iter_dupfixed_many_values_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_many_values_v2() {
    test_iter_dupfixed_many_values_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_dupfixed_from_nonexistent_key_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.put(db, b"aaa", &1u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"ccc", &2u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    // Start from "bbb" which doesn't exist - should find "ccc"
    let results: Vec<(Vec<u8>, [u8; 4])> =
        cursor.iter_dupfixed_from::<Vec<u8>, 4>(b"bbb").unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (b"ccc".to_vec(), 2u32.to_le_bytes()));
}

#[test]
fn test_iter_dupfixed_from_nonexistent_key_v1() {
    test_iter_dupfixed_from_nonexistent_key_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_from_nonexistent_key_v2() {
    test_iter_dupfixed_from_nonexistent_key_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_dupfixed_from_past_end_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.put(db, b"aaa", &1u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    // Start from "zzz" which is past all keys
    let results: Vec<(Vec<u8>, [u8; 4])> =
        cursor.iter_dupfixed_from::<Vec<u8>, 4>(b"zzz").unwrap().map(|r| r.unwrap()).collect();

    assert!(results.is_empty());
}

#[test]
fn test_iter_dupfixed_from_past_end_v1() {
    test_iter_dupfixed_from_past_end_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_from_past_end_v2() {
    test_iter_dupfixed_from_past_end_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

// =============================================================================
// DUPFIXED Single-Key Iterator Tests (iter_dupfixed_of)
// =============================================================================

fn test_iter_dupfixed_of_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create DUPFIXED database with multiple keys
    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

    // Insert values for key1
    txn.put(db, b"key1", &1u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", &2u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"key1", &3u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    // Insert values for key2
    txn.put(db, b"key2", &10u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", &20u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    // Insert values for key3
    txn.put(db, b"key3", &100u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    txn.commit().unwrap();

    // Test: iter_dupfixed_of should only return values for key2
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    let results: Vec<[u8; 4]> =
        cursor.iter_dupfixed_of::<4>(b"key2").unwrap().map(|r| r.unwrap()).collect();

    // Should only contain key2's values
    assert_eq!(results.len(), 2);
    assert_eq!(u32::from_le_bytes(results[0]), 10);
    assert_eq!(u32::from_le_bytes(results[1]), 20);
}

#[test]
fn test_iter_dupfixed_of_v1() {
    test_iter_dupfixed_of_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_of_v2() {
    test_iter_dupfixed_of_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_dupfixed_of_nonexistent_key_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create DUPFIXED database with some data
    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();
    txn.put(db, b"aaa", &1u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.put(db, b"ccc", &2u32.to_le_bytes(), WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    // Seek nonexistent key "bbb" - should return empty iterator
    let results: Vec<[u8; 4]> =
        cursor.iter_dupfixed_of::<4>(b"bbb").unwrap().map(|r| r.unwrap()).collect();

    assert!(results.is_empty());
}

#[test]
fn test_iter_dupfixed_of_nonexistent_key_v1() {
    test_iter_dupfixed_of_nonexistent_key_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_of_nonexistent_key_v2() {
    test_iter_dupfixed_of_nonexistent_key_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_iter_dupfixed_of_many_values_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    use std::collections::HashSet;

    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Create DUPFIXED database with many values to test page boundaries
    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

    // Insert values for "before" key
    txn.put(db, b"before", &999u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    // Insert 1000 values for target key to ensure we span multiple pages
    for i in 0u32..1000 {
        txn.put(db, b"target", &i.to_le_bytes(), WriteFlags::empty()).unwrap();
    }

    // Insert values for "after" key
    txn.put(db, b"zafter", &888u32.to_le_bytes(), WriteFlags::empty()).unwrap();

    txn.commit().unwrap();

    // Read back using iter_dupfixed_of
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    let results: Vec<[u8; 4]> =
        cursor.iter_dupfixed_of::<4>(b"target").unwrap().map(|r| r.unwrap()).collect();

    // Verify count - should be exactly 1000
    assert_eq!(results.len(), 1000);

    // Verify all 1000 values are present (regardless of order)
    let values: HashSet<u32> = results.iter().map(|v| u32::from_le_bytes(*v)).collect();
    let expected: HashSet<u32> = (0u32..1000).collect();
    assert_eq!(values, expected);

    // Verify no values from "before" or "zafter" keys leaked in
    for v in &results {
        let num = u32::from_le_bytes(*v);
        assert!(num < 1000, "unexpected value {num} from other key");
    }
}

#[test]
fn test_iter_dupfixed_of_many_values_v1() {
    test_iter_dupfixed_of_many_values_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_iter_dupfixed_of_many_values_v2() {
    test_iter_dupfixed_of_many_values_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

// Debug assertion tests - only run in debug builds
#[cfg(debug_assertions)]
mod append_debug_tests {
    use super::*;

    #[test]
    #[should_panic(expected = "Append key must be greater")]
    fn test_cursor_append_wrong_order_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // Insert "b" first
        cursor.append(b"b", b"val_b").unwrap();

        // Try to append "a" - should panic because "a" < "b"
        cursor.append(b"a", b"val_a").unwrap();
    }

    #[test]
    #[should_panic(expected = "Append dup must be greater")]
    fn test_cursor_append_dup_wrong_order_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // Insert duplicate "2" first
        cursor.append_dup(b"key", b"2").unwrap();

        // Try to append "1" - should panic because "1" < "2"
        cursor.append_dup(b"key", b"1").unwrap();
    }

    #[test]
    #[should_panic(expected = "Append key must be greater")]
    fn test_tx_append_wrong_order_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Insert "b" first
        txn.append(db, b"b", b"val_b").unwrap();

        // Try to append "a" - should panic because "a" < "b"
        txn.append(db, b"a", b"val_a").unwrap();
    }

    #[test]
    #[should_panic(expected = "Append dup must be greater")]
    fn test_tx_append_dup_wrong_order_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Insert duplicate "2" first
        txn.append_dup(db, b"key", b"2").unwrap();

        // Try to append "1" - should panic because "1" < "2"
        txn.append_dup(db, b"key", b"1").unwrap();
    }

    /// Test that REVERSE_KEY databases work with append.
    ///
    /// REVERSE_KEY means keys are compared from end to beginning, not that
    /// they should be in descending order. The debug assertion is skipped
    /// for REVERSE_KEY databases.
    #[test]
    fn test_cursor_append_reverse_key() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Create REVERSE_KEY database
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::REVERSE_KEY).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // For REVERSE_KEY, keys are compared from end to beginning.
        // Append keys that are in correct order for reverse comparison.
        // The debug assertion is skipped, so MDBX validates the order.
        cursor.append(b"a", b"val_a").unwrap();
        cursor.append(b"b", b"val_b").unwrap();
        cursor.append(b"c", b"val_c").unwrap();

        drop(cursor);
        txn.commit().unwrap();

        // Verify data was written
        let txn = env.begin_ro_sync().unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        let first: Option<(Vec<u8>, Vec<u8>)> = cursor.first().unwrap();
        assert!(first.is_some());
    }

    /// Test that REVERSE_KEY append returns MDBX error for wrong order.
    ///
    /// Since the debug assertion is skipped for REVERSE_KEY databases,
    /// MDBX itself will return KeyMismatch if the order is wrong.
    #[test]
    fn test_cursor_append_reverse_key_wrong_order_returns_error() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Create REVERSE_KEY database
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::REVERSE_KEY).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // Insert "b" first
        cursor.append(b"b", b"val_b").unwrap();

        // Try to append "a" - MDBX should return KeyMismatch
        // (for single-byte keys, reverse comparison is same as normal)
        let result = cursor.append(b"a", b"val_a");
        assert!(result.is_err());
        assert!(matches!(result, Err(MdbxError::KeyMismatch)));
    }

    /// Test that REVERSE_DUP databases work with append_dup.
    ///
    /// REVERSE_DUP means values are compared from end to beginning, not that
    /// they should be in descending order. The debug assertion is skipped
    /// for REVERSE_DUP databases.
    #[test]
    fn test_cursor_append_dup_reverse_dup() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Create REVERSE_DUP database
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::REVERSE_DUP).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // For REVERSE_DUP, values are compared from end to beginning.
        // Append duplicates in correct order for reverse comparison.
        cursor.append_dup(b"key", b"1").unwrap();
        cursor.append_dup(b"key", b"2").unwrap();
        cursor.append_dup(b"key", b"3").unwrap();

        drop(cursor);
        txn.commit().unwrap();

        // Verify data was written
        let txn = env.begin_ro_sync().unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        let first: Option<(Vec<u8>, Vec<u8>)> = cursor.first().unwrap();
        assert!(first.is_some());
    }

    /// Test that REVERSE_DUP append_dup returns MDBX error for wrong order.
    ///
    /// Since the debug assertion is skipped for REVERSE_DUP databases,
    /// MDBX itself will return KeyMismatch if the order is wrong.
    #[test]
    fn test_cursor_append_dup_reverse_dup_wrong_order_returns_error() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Create REVERSE_DUP database
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::REVERSE_DUP).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // Insert "2" first
        cursor.append_dup(b"key", b"2").unwrap();

        // Try to append "1" - MDBX should return KeyMismatch
        // (for single-byte values, reverse comparison is same as normal)
        let result = cursor.append_dup(b"key", b"1");
        assert!(result.is_err());
        assert!(matches!(result, Err(MdbxError::KeyMismatch)));
    }

    // DUP_SORT validation tests - these panic in debug builds
    #[test]
    #[should_panic(expected = "Operation requires DUP_SORT database flag")]
    fn test_first_dup_on_non_dupsort_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();
        txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();
        cursor.first::<(), ()>().unwrap();
        let _ = cursor.first_dup::<()>();
    }

    #[test]
    #[should_panic(expected = "Operation requires DUP_SORT database flag")]
    fn test_append_dup_on_non_dupsort_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();
        let _ = cursor.append_dup(b"key", b"value");
    }

    // DUP_FIXED validation tests - these panic in debug builds
    #[test]
    #[should_panic(expected = "Operation requires DUP_FIXED database flag")]
    fn test_get_multiple_on_non_dupfixed_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
        txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();
        cursor.first::<(), ()>().unwrap();
        let _ = cursor.get_multiple::<()>();
    }

    #[test]
    #[should_panic(expected = "Operation requires DUP_FIXED database flag")]
    fn test_iter_dupfixed_on_non_dupfixed_panics() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
        txn.put(db, b"key", b"value", WriteFlags::empty()).unwrap();
        txn.commit().unwrap();

        let txn = env.begin_ro_sync().unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();
        let _ = cursor.iter_dupfixed_start::<Vec<u8>, 4>();
    }
}
