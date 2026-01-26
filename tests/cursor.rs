#![allow(missing_docs)]
mod common;
use common::{TestRoTxn, TestRwTxn, V1Factory, V2Factory};
use signet_libmdbx::{
    DatabaseFlags, Environment, MdbxError, MdbxResult, ObjectLength, ReadError, ReadResult,
    WriteFlags, tx::TxPtrAccess,
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

    let mut txn = begin_rw(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
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
        let mut txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        for (key, data) in &items {
            txn.put(db, key, data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let mut txn = begin_ro(&env).unwrap();
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
    let mut txn = begin_ro(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
    txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.commit().unwrap();

    let mut txn = begin_ro(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
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
        let mut txn = begin_rw(&env).unwrap();
        for (key, data) in items.clone() {
            let db = txn.open_db(None).unwrap();
            txn.put(db, &key, &data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let mut txn = begin_ro(&env).unwrap();
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
        let mut txn = begin_rw(&env).unwrap();
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
        let mut txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        for (key, data) in &items {
            txn.put(db, key, data, WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }

    let mut txn = begin_rw(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
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

fn test_dup_sort_validation_on_non_dupsort_db_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let mut txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap(); // Non-DUPSORT database
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();

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
fn test_dup_sort_validation_on_non_dupsort_db_v1() {
    test_dup_sort_validation_on_non_dupsort_db_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_dup_sort_validation_on_non_dupsort_db_v2() {
    test_dup_sort_validation_on_non_dupsort_db_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_dup_fixed_validation_on_non_dupfixed_db_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let mut txn = begin_rw(&env).unwrap();
    // Create DUPSORT but NOT DUPFIXED database
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();

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
fn test_dup_fixed_validation_on_non_dupfixed_db_v1() {
    test_dup_fixed_validation_on_non_dupfixed_db_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_dup_fixed_validation_on_non_dupfixed_db_v2() {
    test_dup_fixed_validation_on_non_dupfixed_db_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_dup_sort_methods_work_on_dupsort_db_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let mut txn = begin_rw(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    for i in 0u8..100 {
        txn.put(db, &[i], &[i], WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    let mut txn = begin_ro(&env).unwrap();
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

    let mut txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    for i in 0..n {
        let key = format!("key{i}");
        let data = format!("data{i}");
        txn.put(db, key.as_bytes(), data.as_bytes(), WriteFlags::empty()).unwrap();
    }
    txn.commit().unwrap();

    // Setup like benchmark: transaction and db outside the "iteration"
    let mut txn = begin_ro(&env).unwrap();
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
        fn iterate<A: TxPtrAccess>(
            cursor: &mut signet_libmdbx::Cursor<signet_libmdbx::RO, A>,
        ) -> ReadResult<()> {
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
// Timeout Tests (V2 only with read-tx-timeouts feature)
// =============================================================================

#[cfg(feature = "read-tx-timeouts")]
mod timeout_tests {
    use signet_libmdbx::*;
    use std::time::Duration;
    use tempfile::tempdir;

    const SHORT_TIMEOUT: Duration = Duration::from_millis(100);
    const WAIT_FOR_TIMEOUT: Duration = Duration::from_millis(200);

    /// Test that cursor operations fail after RO transaction times out.
    #[test]
    fn test_cursor_operations_fail_after_timeout() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Write some data
        {
            let txn = env.begin_rw_txn().unwrap();
            let db = txn.open_db(None).unwrap();
            txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
            txn.put(db, b"key2", b"val2", WriteFlags::empty()).unwrap();
            txn.commit().unwrap();
        }

        // Create a v2 transaction with short timeout
        let mut txn = env.begin_ro_single_thread_with_timeout(SHORT_TIMEOUT).unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // Cursor should work before timeout
        assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));

        // Wait for timeout
        std::thread::sleep(WAIT_FOR_TIMEOUT);

        // Cursor operations should now fail with ReadTransactionTimeout
        let err = cursor.first::<(), ()>().unwrap_err();
        assert!(
            matches!(err, ReadError::Mdbx(MdbxError::ReadTransactionTimeout)),
            "Expected ReadTransactionTimeout, got: {err:?}"
        );
    }

    /// Test cursor cleanup after timeout (no memory leak).
    #[test]
    fn test_cursor_cleanup_after_timeout() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Write some data
        {
            let txn = env.begin_rw_txn().unwrap();
            let db = txn.open_db(None).unwrap();
            txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
            txn.commit().unwrap();
        }

        // Create transaction with short timeout
        let mut txn = env.begin_ro_single_thread_with_timeout(SHORT_TIMEOUT).unwrap();
        let db = txn.open_db(None).unwrap();
        let cursor = txn.cursor(db).unwrap();

        // Wait for timeout
        std::thread::sleep(WAIT_FOR_TIMEOUT);

        // Dropping cursor after timeout should not panic
        drop(cursor);
        drop(txn);

        // Environment should still be usable
        let txn = env.begin_ro_txn().unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();
        assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));
    }

    /// Test multiple cursors cleanup after timeout.
    #[test]
    fn test_multiple_cursors_cleanup_after_timeout() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().set_max_dbs(10).open(dir.path()).unwrap();

        // Create databases and write data
        {
            let txn = env.begin_rw_txn().unwrap();
            let db1 = txn.create_db(Some("db1"), DatabaseFlags::empty()).unwrap();
            let db2 = txn.create_db(Some("db2"), DatabaseFlags::empty()).unwrap();
            txn.put(db1, b"k1", b"v1", WriteFlags::empty()).unwrap();
            txn.put(db2, b"k2", b"v2", WriteFlags::empty()).unwrap();
            txn.commit().unwrap();
        }

        // Create transaction with short timeout and multiple cursors
        let mut txn = env.begin_ro_single_thread_with_timeout(SHORT_TIMEOUT).unwrap();
        let db1 = txn.open_db(Some("db1")).unwrap();
        let db2 = txn.open_db(Some("db2")).unwrap();
        let cursor1 = txn.cursor(db1).unwrap();
        let cursor2 = txn.cursor(db2).unwrap();

        // Wait for timeout
        std::thread::sleep(WAIT_FOR_TIMEOUT);

        // Dropping all cursors after timeout should not panic
        drop(cursor1);
        drop(cursor2);
        drop(txn);

        // Verify environment is still usable
        let txn = env.begin_ro_txn().unwrap();
        let db = txn.open_db(Some("db1")).unwrap();
        let mut cursor = txn.cursor(db).unwrap();
        assert_eq!(cursor.first().unwrap(), Some((*b"k1", *b"v1")));
    }

    /// Test that transactions without timeout work correctly.
    #[test]
    fn test_no_timeout_transaction_works() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Write data
        {
            let txn = env.begin_rw_txn().unwrap();
            let db = txn.open_db(None).unwrap();
            txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
            txn.commit().unwrap();
        }

        // Create transaction without timeout
        let mut txn = env.begin_ro_single_thread_no_timeout().unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // Cursor should work before the "normal" timeout period
        assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));

        // Wait longer than the short timeout (but not forever)
        std::thread::sleep(WAIT_FOR_TIMEOUT);

        // Cursor should still work since there's no timeout
        assert_eq!(cursor.first().unwrap(), Some((*b"key1", *b"val1")));
    }

    /// Test iterator behavior after timeout.
    #[test]
    fn test_iter_fails_after_timeout() {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();

        // Write data
        {
            let txn = env.begin_rw_txn().unwrap();
            let db = txn.open_db(None).unwrap();
            for i in 0u8..10 {
                txn.put(db, [i], [i], WriteFlags::empty()).unwrap();
            }
            txn.commit().unwrap();
        }

        // Create transaction with short timeout
        let mut txn = env.begin_ro_single_thread_with_timeout(SHORT_TIMEOUT).unwrap();
        let db = txn.open_db(None).unwrap();
        let mut cursor = txn.cursor(db).unwrap();

        // Start iterating before timeout
        let mut iter = cursor.iter::<[u8; 1], [u8; 1]>();
        assert!(iter.next().unwrap().is_ok());

        // Wait for timeout
        std::thread::sleep(WAIT_FOR_TIMEOUT);

        // Iterator should return timeout error
        let err = iter.next().unwrap().unwrap_err();
        assert!(
            matches!(err, ReadError::Mdbx(MdbxError::ReadTransactionTimeout)),
            "Expected ReadTransactionTimeout, got: {err:?}"
        );
    }
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
    let mut txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    cursor.append(b"a", b"val_a").unwrap();
    cursor.append(b"b", b"val_b").unwrap();
    cursor.append(b"c", b"val_c").unwrap();

    drop(cursor);
    txn.commit().unwrap();

    // Verify data was written correctly
    let mut txn = begin_ro(&env).unwrap();
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
        let mut txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();

        txn.append(db, b"key1", b"val1").unwrap();
        txn.append(db, b"key2", b"val2").unwrap();
        txn.append(db, b"key3", b"val3").unwrap();

        txn.commit().unwrap();
    }

    // Verify data was written correctly
    let mut txn = begin_ro(&env).unwrap();
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
    let mut txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();
    let mut cursor = txn.cursor(db).unwrap();

    // Append duplicates for key "a" in sorted order
    cursor.append_dup(b"a", b"1").unwrap();
    cursor.append_dup(b"a", b"2").unwrap();
    cursor.append_dup(b"a", b"3").unwrap();

    drop(cursor);
    txn.commit().unwrap();

    // Verify
    let mut txn = begin_ro(&env).unwrap();
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

fn test_append_dup_requires_dupsort_impl<RwTx>(begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>)
where
    RwTx: TestRwTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Try append_dup on non-DUPSORT database
    let mut txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap(); // Non-DUPSORT database
    let mut cursor = txn.cursor(db).unwrap();

    // Should return RequiresDupSort error
    let err = cursor.append_dup(b"key", b"value").unwrap_err();
    assert!(matches!(err, MdbxError::RequiresDupSort));
}

#[test]
fn test_append_dup_requires_dupsort_v1() {
    test_append_dup_requires_dupsort_impl(V1Factory::begin_rw);
}

#[test]
fn test_append_dup_requires_dupsort_v2() {
    test_append_dup_requires_dupsort_impl(V2Factory::begin_rw);
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

        let txn = env.begin_rw_txn().unwrap();
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

        let txn = env.begin_rw_txn().unwrap();
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

        let txn = env.begin_rw_txn().unwrap();
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

        let txn = env.begin_rw_txn().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Insert duplicate "2" first
        txn.append_dup(db, b"key", b"2").unwrap();

        // Try to append "1" - should panic because "1" < "2"
        txn.append_dup(db, b"key", b"1").unwrap();
    }
}
