#![allow(missing_docs)]
use signet_libmdbx::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use tempfile::tempdir;

#[test]
fn begin_ro_sync_multi_all_same_snapshot() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_sync().unwrap();
    let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
    txn.put(db, b"k", b"v", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let txns = env.begin_ro_sync_multi(4).unwrap();
    assert_eq!(txns.len(), 4);

    let ids: Vec<u64> = txns.iter().map(|tx| tx.id().unwrap()).collect();
    assert!(ids.windows(2).all(|w| w[0] == w[1]));
}

#[test]
fn begin_ro_unsync_multi_all_same_snapshot() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_sync().unwrap();
    let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
    txn.put(db, b"k", b"v", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let txns = env.begin_ro_unsync_multi(4).unwrap();
    assert_eq!(txns.len(), 4);

    let ids: Vec<u64> = txns.iter().map(|tx| tx.id().unwrap()).collect();
    assert!(ids.windows(2).all(|w| w[0] == w[1]));
}

#[test]
fn begin_ro_sync_multi_zero_returns_empty() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txns = env.begin_ro_sync_multi(0).unwrap();
    assert!(txns.is_empty());
}

#[test]
fn begin_ro_unsync_multi_zero_returns_empty() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txns = env.begin_ro_unsync_multi(0).unwrap();
    assert!(txns.is_empty());
}

#[test]
fn begin_ro_sync_multi_one_returns_single() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txns = env.begin_ro_sync_multi(1).unwrap();
    assert_eq!(txns.len(), 1);
}

#[test]
fn begin_ro_unsync_multi_one_returns_single() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txns = env.begin_ro_unsync_multi(1).unwrap();
    assert_eq!(txns.len(), 1);
}

#[test]
fn begin_ro_sync_multi_consistent_under_write_pressure() {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Seed the database
    let txn = env.begin_rw_sync().unwrap();
    let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
    txn.put(db, b"k", b"v", WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    // Writer thread: commit as fast as possible to create snapshot churn
    let writer = {
        let env = env.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let txn = env.begin_rw_sync().unwrap();
                let db = txn.open_db(None).unwrap();
                txn.put(db, b"counter", i.to_le_bytes(), WriteFlags::empty()).unwrap();
                txn.commit().unwrap();
                i += 1;
            }
        })
    };

    // Open multi-txn sets concurrently with writer
    for _ in 0..20 {
        let txns = env.begin_ro_sync_multi(4).unwrap();
        let ids: Vec<u64> = txns.iter().map(|tx| tx.id().unwrap()).collect();
        assert!(ids.windows(2).all(|w| w[0] == w[1]), "snapshot divergence detected: {ids:?}");
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}
