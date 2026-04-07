#![no_main]
use libfuzzer_sys::fuzz_target;
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use std::borrow::Cow;
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    // Need at least one byte to split key/value.
    if data.is_empty() {
        return;
    }

    // Use first byte as split point for key vs value.
    let split = (data[0] as usize).min(data.len().saturating_sub(1));
    let (key, value) = data[1..].split_at(split.min(data.len().saturating_sub(1)));

    // Keys must be non-empty for MDBX.
    if key.is_empty() {
        return;
    }

    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Write in RW transaction, then read back as Cow (dirty page path).
    let txn = env.begin_rw_unsync().unwrap();
    let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
    txn.put(db, key, value, WriteFlags::empty()).unwrap();

    // Read while transaction is still open: data is on a dirty page, so
    // Cow::decode_borrow should return Cow::Owned.
    let readback: Option<Cow<'_, [u8]>> = txn.get(db.dbi(), key).unwrap();
    let readback = readback.unwrap();
    assert_eq!(readback.as_ref(), value);

    txn.commit().unwrap();

    // Read via RO transaction: data is on a clean page, so Cow should borrow.
    let ro_txn = env.begin_ro_unsync().unwrap();
    let ro_db = ro_txn.open_db(None).unwrap();
    let clean: Option<Cow<'_, [u8]>> = ro_txn.get(ro_db.dbi(), key).unwrap();
    let clean = clean.unwrap();
    assert_eq!(clean.as_ref(), value);
});
