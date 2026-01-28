#![allow(missing_docs)]
use byteorder::{ByteOrder, LittleEndian};
use signet_libmdbx::*;
use tempfile::tempdir;

#[test]
fn test_open() {
    let dir = tempdir().unwrap();

    // opening non-existent env with read-only should fail
    assert!(Environment::builder().set_flags(Mode::ReadOnly.into()).open(dir.path()).is_err());

    // opening non-existent env should succeed
    Environment::builder().open(dir.path()).unwrap();

    // opening env with read-only should succeed
    Environment::builder().set_flags(Mode::ReadOnly.into()).open(dir.path()).unwrap();
}

#[test]
fn test_begin_txn() {
    let dir = tempdir().unwrap();

    {
        // writable environment
        let env = Environment::builder().open(dir.path()).unwrap();

        env.begin_rw_sync().unwrap();
        env.begin_ro_sync().unwrap();
    }

    {
        // read-only environment
        let env = Environment::builder().set_flags(Mode::ReadOnly.into()).open(dir.path()).unwrap();

        env.begin_rw_sync().unwrap_err();
        env.begin_ro_sync().unwrap();
    }
}

#[test]
fn test_open_db() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(1).open(dir.path()).unwrap();

    let txn = env.begin_ro_sync().unwrap();
    txn.open_db(None).unwrap();
    txn.open_db(Some("testdb")).unwrap_err();
}

#[test]
fn test_create_db() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(11).open(dir.path()).unwrap();

    let txn = env.begin_rw_sync().unwrap();
    txn.open_db(Some("testdb")).unwrap_err();
    txn.create_db(Some("testdb"), DatabaseFlags::empty()).unwrap();
    txn.open_db(Some("testdb")).unwrap();
}

#[test]
fn test_close_database() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(10).open(dir.path()).unwrap();

    let txn = env.begin_rw_sync().unwrap();
    txn.create_db(Some("db"), DatabaseFlags::empty()).unwrap();
    txn.open_db(Some("db")).unwrap();
}

#[test]
fn test_sync() {
    let dir = tempdir().unwrap();
    {
        let env = Environment::builder().open(dir.path()).unwrap();
        env.sync(true).unwrap();
    }
    {
        let env = Environment::builder().set_flags(Mode::ReadOnly.into()).open(dir.path()).unwrap();
        env.sync(true).unwrap_err();
    }
}

#[test]
fn test_stat() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Stats should be empty initially.
    let stat = env.stat().unwrap();
    assert_eq!(stat.depth(), 0);
    assert_eq!(stat.branch_pages(), 0);
    assert_eq!(stat.leaf_pages(), 0);
    assert_eq!(stat.overflow_pages(), 0);
    assert_eq!(stat.entries(), 0);

    // Write a few small values.
    for i in 0..64 {
        let mut value = [0u8; 8];
        LittleEndian::write_u64(&mut value, i);
        let tx = env.begin_rw_sync().expect("begin_rw_sync");
        let db = tx.open_db(None).unwrap();
        tx.put(db, value, value, WriteFlags::default()).expect("tx.put");
        tx.commit().expect("tx.commit");
    }

    // Stats should now reflect inserted values.
    let stat = env.stat().unwrap();
    assert_eq!(stat.depth(), 1);
    assert_eq!(stat.branch_pages(), 0);
    assert_eq!(stat.leaf_pages(), 1);
    assert_eq!(stat.overflow_pages(), 0);
    assert_eq!(stat.entries(), 64);
}

#[test]
fn test_info() {
    let map_size = 1024 * 1024;
    let dir = tempdir().unwrap();
    let env = Environment::builder()
        .set_geometry(Geometry { size: Some(map_size..), ..Default::default() })
        .open(dir.path())
        .unwrap();

    let info = env.info().unwrap();
    assert_eq!(info.geometry().min(), map_size as u64);
    // assert_eq!(info.last_pgno(), 1);
    // assert_eq!(info.last_txnid(), 0);
    assert_eq!(info.num_readers(), 0);
    assert!(matches!(info.mode(), Mode::ReadWrite { sync_mode: SyncMode::Durable }));
    assert!(env.is_read_write().unwrap());

    drop(env);
    let env = Environment::builder()
        .set_geometry(Geometry { size: Some(map_size..), ..Default::default() })
        .set_flags(EnvironmentFlags { mode: Mode::ReadOnly, ..Default::default() })
        .open(dir.path())
        .unwrap();
    let info = env.info().unwrap();
    assert!(matches!(info.mode(), Mode::ReadOnly));
    assert!(env.is_read_only().unwrap());
}

#[test]
fn test_freelist() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let mut freelist = env.freelist().unwrap();
    assert_eq!(freelist, 0);

    // Write a few small values.
    for i in 0..64 {
        let mut value = [0u8; 8];
        LittleEndian::write_u64(&mut value, i);
        let tx = env.begin_rw_sync().expect("begin_rw_sync");
        let db = tx.open_db(None).unwrap();
        tx.put(db, value, value, WriteFlags::default()).expect("tx.put");
        tx.commit().expect("tx.commit");
    }
    let tx = env.begin_rw_sync().expect("begin_rw_sync");
    let db = tx.open_db(None).unwrap();
    tx.clear_db(db).expect("clear");
    tx.commit().expect("tx.commit");

    // Freelist should not be empty after clear_db.
    freelist = env.freelist().unwrap();
    assert!(freelist > 0);
}
