#![no_main]
use libfuzzer_sys::fuzz_target;
use signet_libmdbx::{DatabaseFlags, Environment, MdbxError, ReadError, WriteFlags};
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    // Need at least one byte for a key.
    if data.is_empty() {
        return;
    }

    // Use first byte as key length (1..=16), rest is the value.
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

    // Attempt decoding as fixed-size arrays. Length mismatches must produce an
    // error, never a panic.
    let r4: Result<Option<[u8; 4]>, ReadError> = ro_txn.get(ro_db.dbi(), key);
    let r8: Result<Option<[u8; 8]>, ReadError> = ro_txn.get(ro_db.dbi(), key);
    let r16: Result<Option<[u8; 16]>, ReadError> = ro_txn.get(ro_db.dbi(), key);
    let r32: Result<Option<[u8; 32]>, ReadError> = ro_txn.get(ro_db.dbi(), key);

    // Validate: correct length → Ok, wrong length → DecodeErrorLenDiff.
    for (result, expected_len) in [
        (r4.map(|o| o.map(|a| a.len())), 4usize),
        (r8.map(|o| o.map(|a| a.len())), 8),
        (r16.map(|o| o.map(|a| a.len())), 16),
        (r32.map(|o| o.map(|a| a.len())), 32),
    ] {
        match result {
            Ok(Some(len)) => assert_eq!(len, expected_len),
            Ok(None) => {}
            Err(ReadError::Mdbx(MdbxError::DecodeErrorLenDiff)) => {
                // Expected when value.len() != expected_len.
                assert_ne!(value.len(), expected_len);
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
});
