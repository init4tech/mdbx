#![no_main]
use libfuzzer_sys::fuzz_target;
use signet_libmdbx::{DatabaseFlags, Environment, ObjectLength, WriteFlags};
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    // Need at least 1 byte for the key.
    if data.is_empty() {
        return;
    }

    // First byte: key length (1..=16).
    let key_len = ((data[0] as usize) % 16) + 1;
    if data.len() < 1 + key_len {
        return;
    }
    let key = &data[1..1 + key_len];
    let value = &data[1 + key_len..];

    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    let txn = env.begin_rw_unsync().unwrap();
    let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
    txn.put(db, key, value, WriteFlags::empty()).unwrap();
    txn.commit().unwrap();

    let ro_txn = env.begin_ro_unsync().unwrap();
    let ro_db = ro_txn.open_db(None).unwrap();

    // ObjectLength must return the exact byte length of the stored value.
    let len: Option<ObjectLength> = ro_txn.get(ro_db.dbi(), key).unwrap();
    let len = len.unwrap();
    assert_eq!(*len, value.len());
});
