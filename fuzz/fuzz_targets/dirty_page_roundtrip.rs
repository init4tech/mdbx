#![no_main]
use libfuzzer_sys::fuzz_target;
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use tempfile::tempdir;

/// Near-page-boundary value sizes to probe is_dirty_raw behaviour.
const BIASED_SIZES: [usize; 4] = [4094, 4096, 4098, 0];

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }

    // First byte selects value-size bias; remaining bytes provide content.
    let bias_idx = (data[0] as usize) % BIASED_SIZES.len();
    let biased_size = BIASED_SIZES[bias_idx];
    let content = &data[1..];

    // Build value: if biased_size > 0, pad/trim content to that size.
    let value: Vec<u8> = if biased_size > 0 {
        let mut v = content.to_vec();
        v.resize(biased_size, 0xAB);
        v
    } else {
        content.to_vec()
    };

    // Key is always the first 4 bytes of content (or padded).
    let mut key = [0u8; 4];
    let copy_len = content.len().min(4);
    key[..copy_len].copy_from_slice(&content[..copy_len]);

    let dir = tempdir().unwrap();
    let env = Environment::builder()
        .set_geometry(signet_libmdbx::Geometry {
            size: Some(0..(1024 * 1024 * 64)),
            ..Default::default()
        })
        .open(dir.path())
        .unwrap();

    // Write in RW transaction; read back on dirty page.
    // We use Vec<u8> to force a copy out of the transaction before commit.
    let dirty_bytes: Vec<u8> = {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::empty()).unwrap();
        txn.put(db, &key, &value, WriteFlags::empty()).unwrap();

        // Read while dirty; Vec<u8> always copies, so no lifetime tie to txn.
        let dirty: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
        let dirty = dirty.unwrap();
        assert_eq!(dirty.as_slice(), value.as_slice());

        txn.commit().unwrap();
        dirty
    };

    // Read via RO transaction: data now on a clean page.
    let ro_txn = env.begin_ro_unsync().unwrap();
    let ro_db = ro_txn.open_db(None).unwrap();
    let clean: Option<Vec<u8>> = ro_txn.get(ro_db.dbi(), &key).unwrap();
    let clean = clean.unwrap();

    // Both reads must agree on value content.
    assert_eq!(dirty_bytes.as_slice(), clean.as_slice());
});
