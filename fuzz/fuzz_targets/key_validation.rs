#![no_main]
use libfuzzer_sys::fuzz_target;
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let dir = tempdir().unwrap();
    // Two named databases require set_max_dbs(2) on the environment.
    let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

    let txn = env.begin_rw_unsync().unwrap();

    // Database 1: default (no special flags). Accepts arbitrary byte keys.
    let default_db = txn.create_db(None, DatabaseFlags::empty()).unwrap();

    // Database 2: INTEGER_KEY. Requires 4- or 8-byte aligned keys.
    let int_db =
        txn.create_db(Some("intkeys"), DatabaseFlags::INTEGER_KEY | DatabaseFlags::CREATE).unwrap();

    // Attempt put with the raw fuzz bytes as key. Should either succeed or
    // return a typed error — never panic.
    let _ = txn.put(default_db, data, b"value", WriteFlags::empty());

    // INTEGER_KEY requires exactly 4 or 8 byte keys. MDBX aborts (not
    // errors) on invalid sizes, so only feed valid-length keys to this db.
    // We still fuzz the *content* of those keys.
    if data.len() == 4 || data.len() == 8 {
        let _ = txn.put(int_db, data, b"value", WriteFlags::empty());
        let _: signet_libmdbx::ReadResult<Option<Vec<u8>>> = txn.get(int_db.dbi(), data);
    }

    // Attempt get with fuzz bytes as key on the default database.
    let _: signet_libmdbx::ReadResult<Option<Vec<u8>>> = txn.get(default_db.dbi(), data);
});
