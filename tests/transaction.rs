#![allow(missing_docs)]
mod common;
use common::{TestRoTxn, TestRwTxn, V1Factory, V2Factory};
use signet_libmdbx::*;
use std::{
    borrow::Cow,
    io::Write,
    sync::{Arc, Barrier},
    thread::{self, JoinHandle},
};
use tempfile::tempdir;

// =============================================================================
// Dual-variant tests (run for both V1 and V2)
// =============================================================================

fn test_put_get_del_impl<RwTx, RoTx>(
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
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key3", b"val3", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    assert_eq!(txn.get(db.dbi(), b"key1").unwrap(), Some(*b"val1"));
    assert_eq!(txn.get(db.dbi(), b"key2").unwrap(), Some(*b"val2"));
    assert_eq!(txn.get(db.dbi(), b"key3").unwrap(), Some(*b"val3"));
    assert_eq!(txn.get::<()>(db.dbi(), b"key").unwrap(), None);

    txn.del(db, b"key1", None).unwrap();
    assert_eq!(txn.get::<()>(db.dbi(), b"key1").unwrap(), None);
}

#[test]
fn test_put_get_del_v1() {
    test_put_get_del_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_put_get_del_v2() {
    test_put_get_del_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_put_get_del_multi_impl<RwTx, RoTx>(
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
    txn.put(db, b"key3", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key3", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key3", b"val3", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    {
        let mut cur = txn.cursor(db).unwrap();
        // iter_dup_of now yields just values, not (key, value) tuples
        let iter = cur.iter_dup_of::<[u8; 4]>(b"key1").unwrap();
        let vals = iter.map(|x| x.unwrap()).collect::<Vec<_>>();
        assert_eq!(vals, vec![*b"val1", *b"val2", *b"val3"]);
    }
    txn.commit().unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    txn.del(db, b"key1", Some(b"val2")).unwrap();
    txn.del(db, b"key2", None).unwrap();
    txn.commit().unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    {
        let mut cur = txn.cursor(db).unwrap();
        // iter_dup_of now yields just values, not (key, value) tuples
        let iter = cur.iter_dup_of::<[u8; 4]>(b"key1").unwrap();
        let vals = iter.map(|x| x.unwrap()).collect::<Vec<_>>();
        assert_eq!(vals, vec![*b"val1", *b"val3"]);

        let iter = cur.iter_dup_of::<[u8; 4]>(b"key2").unwrap();
        assert_eq!(0, iter.count());
    }
    txn.commit().unwrap();
}

#[test]
fn test_put_get_del_multi_v1() {
    test_put_get_del_multi_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_put_get_del_multi_v2() {
    test_put_get_del_multi_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_put_get_del_empty_key_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    _begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, Default::default()).unwrap();
    txn.put(db, b"", b"hello", WriteFlags::empty()).unwrap();
    assert_eq!(txn.get(db.dbi(), b"").unwrap(), Some(*b"hello"));
    txn.commit().unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    assert_eq!(txn.get(db.dbi(), b"").unwrap(), Some(*b"hello"));
    txn.put(db, b"", b"", WriteFlags::empty()).unwrap();
    assert_eq!(txn.get(db.dbi(), b"").unwrap(), Some(*b""));
}

#[test]
fn test_put_get_del_empty_key_v1() {
    test_put_get_del_empty_key_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_put_get_del_empty_key_v2() {
    test_put_get_del_empty_key_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_clear_db_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    {
        let txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        txn.put(db, b"key", b"val", WriteFlags::empty()).unwrap();
        txn.commit().unwrap();
    }

    {
        let txn = begin_rw(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        txn.clear_db(db).unwrap();
        txn.commit().unwrap();
    }

    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    assert_eq!(txn.get::<()>(db.dbi(), b"key").unwrap(), None);
}

#[test]
fn test_clear_db_v1() {
    test_clear_db_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_clear_db_v2() {
    test_clear_db_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_drop_db_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    {
        let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

        {
            let txn = begin_rw(&env).unwrap();
            let db = txn.create_db(Some("test"), DatabaseFlags::empty()).unwrap();
            txn.put(db, b"key", b"val", WriteFlags::empty()).unwrap();
            // Workaround for MDBX dbi drop issue
            txn.create_db(Some("canary"), DatabaseFlags::empty()).unwrap();
            txn.commit().unwrap();
        }
        {
            let txn = begin_rw(&env).unwrap();
            let db = txn.open_db(Some("test")).unwrap();
            unsafe {
                txn.drop_db(db).unwrap();
            }
            assert!(matches!(txn.open_db(Some("test")).unwrap_err(), MdbxError::NotFound));
            txn.commit().unwrap();
        }
    }

    let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

    let txn = begin_ro(&env).unwrap();
    txn.open_db(Some("canary")).unwrap();
    assert!(matches!(txn.open_db(Some("test")).unwrap_err(), MdbxError::NotFound));
}

#[test]
fn test_drop_db_v1() {
    test_drop_db_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_drop_db_v2() {
    test_drop_db_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_stat_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key2", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key3", b"val3", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    {
        let txn = begin_ro(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        let stat = txn.db_stat(db.dbi()).unwrap();
        assert_eq!(stat.entries(), 3);
    }

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    txn.del(db, b"key1", None).unwrap();
    txn.del(db, b"key2", None).unwrap();
    txn.commit().unwrap();

    {
        let txn = begin_ro(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        let stat = txn.db_stat(db.dbi()).unwrap();
        assert_eq!(stat.entries(), 1);
    }

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    txn.put(db, b"key4", b"val4", WriteFlags::empty()).unwrap();
    txn.put(db, b"key5", b"val5", WriteFlags::empty()).unwrap();
    txn.put(db, b"key6", b"val6", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    {
        let txn = begin_ro(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        let stat = txn.db_stat(db.dbi()).unwrap();
        assert_eq!(stat.entries(), 4);
    }
}

#[test]
fn test_stat_v1() {
    test_stat_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_stat_v2() {
    test_stat_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_stat_dupsort_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
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
    txn.put(db, b"key3", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key3", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key3", b"val3", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    {
        let txn = begin_ro(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        let stat = txn.db_stat(db.dbi()).unwrap();
        assert_eq!(stat.entries(), 9);
    }

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    txn.del(db, b"key1", Some(b"val2")).unwrap();
    txn.del(db, b"key2", None).unwrap();
    txn.commit().unwrap();

    {
        let txn = begin_ro(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        let stat = txn.db_stat(db.dbi()).unwrap();
        assert_eq!(stat.entries(), 5);
    }

    let txn = begin_rw(&env).unwrap();
    let db = txn.open_db(None).unwrap();
    txn.put(db, b"key4", b"val1", WriteFlags::empty()).unwrap();
    txn.put(db, b"key4", b"val2", WriteFlags::empty()).unwrap();
    txn.put(db, b"key4", b"val3", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    {
        let txn = begin_ro(&env).unwrap();
        let db = txn.open_db(None).unwrap();
        let stat = txn.db_stat(db.dbi()).unwrap();
        assert_eq!(stat.entries(), 8);
    }
}

#[test]
fn test_stat_dupsort_v1() {
    test_stat_dupsort_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_stat_dupsort_v2() {
    test_stat_dupsort_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_open_db_cached_returns_same_handle_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    txn.create_db(Some("test"), DatabaseFlags::empty()).unwrap();
    txn.commit().unwrap();

    // Test that open_db_cached returns the same dbi for the same name
    let txn = begin_ro(&env).unwrap();

    let db1 = txn.open_db(None).unwrap();
    let db2 = txn.open_db(None).unwrap();
    assert_eq!(db1.dbi(), db2.dbi());

    let db3 = txn.open_db(Some("test")).unwrap();
    let db4 = txn.open_db(Some("test")).unwrap();
    assert_eq!(db3.dbi(), db4.dbi());

    // Different names should have different dbis
    assert_ne!(db1.dbi(), db3.dbi());
}

#[test]
fn test_open_db_cached_returns_same_handle_v1() {
    test_open_db_cached_returns_same_handle_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_open_db_cached_returns_same_handle_v2() {
    test_open_db_cached_returns_same_handle_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_database_flags_getter_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();

    // Create a database with DUP_SORT flag
    let db = txn.create_db(Some("dupsort"), DatabaseFlags::DUP_SORT).unwrap();
    assert!(db.flags().contains(DatabaseFlags::DUP_SORT));

    // Create a database without special flags
    let db_default = txn.create_db(Some("default"), DatabaseFlags::empty()).unwrap();
    assert!(!db_default.flags().contains(DatabaseFlags::DUP_SORT));

    txn.commit().unwrap();

    // Verify flags persist after reopening
    let txn = begin_ro(&env).unwrap();
    let db = txn.open_db(Some("dupsort")).unwrap();
    assert!(db.flags().contains(DatabaseFlags::DUP_SORT));
}

#[test]
fn test_database_flags_getter_v1() {
    test_database_flags_getter_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_database_flags_getter_v2() {
    test_database_flags_getter_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

fn test_cached_db_has_correct_flags_impl<RwTx, RoTx>(
    begin_rw: impl Fn(&Environment) -> MdbxResult<RwTx>,
    begin_ro: impl Fn(&Environment) -> MdbxResult<RoTx>,
) where
    RwTx: TestRwTxn,
    RoTx: TestRoTxn,
{
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

    let txn = begin_rw(&env).unwrap();
    txn.create_db(Some("dupsort"), DatabaseFlags::DUP_SORT).unwrap();
    txn.commit().unwrap();

    // Test that cached open returns correct flags
    let txn = begin_ro(&env).unwrap();

    // First open - cache miss, goes through FFI
    let db1 = txn.open_db(Some("dupsort")).unwrap();
    assert!(db1.flags().contains(DatabaseFlags::DUP_SORT));

    // Second open - cache hit
    let db2 = txn.open_db(Some("dupsort")).unwrap();
    assert!(db2.flags().contains(DatabaseFlags::DUP_SORT));

    // Verify they return the same handle
    assert_eq!(db1.dbi(), db2.dbi());
}

#[test]
fn test_cached_db_has_correct_flags_v1() {
    test_cached_db_has_correct_flags_impl(V1Factory::begin_rw, V1Factory::begin_ro);
}

#[test]
fn test_cached_db_has_correct_flags_v2() {
    test_cached_db_has_correct_flags_impl(V2Factory::begin_rw, V2Factory::begin_ro);
}

// =============================================================================
// V1-only tests (require features not supported by V2)
// =============================================================================

/// Test reserve - V1 only because reserve returns different types
#[test]
fn test_reserve_v1() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_sync().unwrap();
    let db = txn.open_db(None).unwrap();
    {
        unsafe {
            // SAFETY: the returned slice is used before the transaction is committed or aborted.
            let mut writer = txn.reserve(db, b"key1", 4, WriteFlags::empty()).unwrap();
            writer.write_all(b"val1").unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = env.begin_rw_sync().unwrap();
    let db = txn.open_db(None).unwrap();
    assert_eq!(txn.get(db.dbi(), b"key1").unwrap(), Some(*b"val1"));
    assert_eq!(txn.get::<()>(db.dbi(), b"key").unwrap(), None);

    txn.del(db, b"key1", None).unwrap();
    assert_eq!(txn.get::<()>(db.dbi(), b"key1").unwrap(), None);
}

/// Test reserve - V2 version
#[test]
fn test_reserve_v2() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_unsync().unwrap();
    let db = txn.open_db(None).unwrap();
    {
        unsafe {
            // SAFETY: the returned slice is used before the transaction is committed or aborted.
            let mut writer = txn.reserve(db, b"key1", 4, WriteFlags::empty()).unwrap();
            writer.write_all(b"val1").unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = env.begin_rw_unsync().unwrap();
    let db = txn.open_db(None).unwrap();
    assert_eq!(txn.get(db.dbi(), b"key1").unwrap(), Some(*b"val1"));
    assert_eq!(txn.get::<()>(db.dbi(), b"key").unwrap(), None);

    txn.del(db, b"key1", None).unwrap();
    assert_eq!(txn.get::<()>(db.dbi(), b"key1").unwrap(), None);
}

/// Test nested transactions - V1 only (V2 doesn't support nested txns)
#[test]
fn test_nested_txn() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_sync().unwrap();
    let db = txn.open_db(None).unwrap();
    txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();

    {
        let nested = txn.begin_nested_txn().unwrap();
        let db = nested.open_db(None).unwrap();
        nested.put(db, b"key2", b"val2", WriteFlags::empty()).unwrap();
        assert_eq!(nested.get(db.dbi(), b"key1").unwrap(), Some(*b"val1"));
        assert_eq!(nested.get(db.dbi(), b"key2").unwrap(), Some(*b"val2"));
    }

    let db = txn.open_db(None).unwrap();
    assert_eq!(txn.get(db.dbi(), b"key1").unwrap(), Some(*b"val1"));
    assert_eq!(txn.get::<()>(db.dbi(), b"key2").unwrap(), None);
}

/// Test concurrent readers with single writer - V1 only (V2 is !Sync)
#[test]
fn test_concurrent_readers_single_writer() {
    let dir = tempdir().unwrap();
    let env: Arc<Environment> = Arc::new(Environment::builder().open(dir.path()).unwrap());

    let n = 10usize; // Number of concurrent readers
    let barrier = Arc::new(Barrier::new(n + 1));
    let mut threads: Vec<JoinHandle<bool>> = Vec::with_capacity(n);

    let key = b"key";
    let val = b"val";

    for _ in 0..n {
        let reader_env = env.clone();
        let reader_barrier = barrier.clone();

        threads.push(thread::spawn(move || {
            {
                let txn = reader_env.begin_ro_sync().unwrap();
                let db = txn.open_db(None).unwrap();
                assert_eq!(txn.get::<()>(db.dbi(), key).unwrap(), None);
            }
            reader_barrier.wait();
            reader_barrier.wait();
            {
                let txn = reader_env.begin_ro_sync().unwrap();
                let db = txn.open_db(None).unwrap();
                txn.get::<[u8; 3]>(db.dbi(), key).unwrap().unwrap() == *val
            }
        }));
    }

    let txn = env.begin_rw_sync().unwrap();
    let db = txn.open_db(None).unwrap();

    barrier.wait();
    txn.put(db, key, val, WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    barrier.wait();

    assert!(threads.into_iter().all(|b| b.join().unwrap()))
}

/// Test concurrent writers - V1 only (V2 is !Send)
#[test]
fn test_concurrent_writers() {
    let dir = tempdir().unwrap();
    let env = Arc::new(Environment::builder().open(dir.path()).unwrap());

    let n = 10usize; // Number of concurrent writers
    let mut threads: Vec<JoinHandle<bool>> = Vec::with_capacity(n);

    let key = "key";
    let val = "val";

    for i in 0..n {
        let writer_env = env.clone();

        threads.push(thread::spawn(move || {
            let txn = writer_env.begin_rw_sync().unwrap();
            let db = txn.open_db(None).unwrap();
            txn.put(db, format!("{key}{i}"), format!("{val}{i}"), WriteFlags::empty()).unwrap();
            txn.commit().is_ok()
        }));
    }
    assert!(threads.into_iter().all(|b| b.join().unwrap()));

    let txn = env.begin_ro_sync().unwrap();
    let db = txn.open_db(None).unwrap();

    for i in 0..n {
        assert_eq!(
            Cow::<Vec<u8>>::Owned(format!("{val}{i}").into_bytes()),
            txn.get(db.dbi(), format!("{key}{i}").as_bytes()).unwrap().unwrap()
        );
    }
}
