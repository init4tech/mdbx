#![no_main]
use libfuzzer_sys::fuzz_target;
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }

    // First byte: value size in the range 4..=64 (must be uniform across all
    // values in a DUP_FIXED database).
    let value_size = (data[0] as usize % 61) + 4;
    // Second byte: number of values to insert, clamped to 1..=100.
    let n_values = (data[1] as usize % 100) + 1;
    let payload = &data[2..];

    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();

    // Write phase: insert values, read back dirty page bytes, then commit.
    let (dirty_len, inserted_count) = {
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        // Build n_values distinct fixed-size values from the fuzz payload,
        // padding or cycling as needed. We deduplicate before inserting:
        // MDBX ignores exact duplicate key+value pairs silently.
        let mut inserted: Vec<Vec<u8>> = Vec::with_capacity(n_values);
        for i in 0..n_values {
            let mut val = vec![0u8; value_size];
            // Fill from payload, cycling, then XOR with index for uniqueness.
            for (j, byte) in val.iter_mut().enumerate() {
                let src = if payload.is_empty() {
                    0
                } else {
                    payload[(i * value_size + j) % payload.len()]
                };
                *byte = src ^ ((i & 0xFF) as u8);
            }
            // Skip if we already have this exact value.
            if !inserted.contains(&val) {
                if txn.put(db, b"key", &val, WriteFlags::empty()).is_ok() {
                    inserted.push(val);
                }
            }
        }

        if inserted.is_empty() {
            return;
        }

        // Read back via cursor while in the write transaction (dirty page).
        // Use Vec<u8> so there is no lifetime tie to the transaction.
        let dirty_len = {
            let mut cursor = txn.cursor(db).unwrap();
            cursor.first::<(), ()>().unwrap();
            let dirty: Option<Vec<u8>> = cursor.get_multiple().unwrap();
            let page = dirty.unwrap();
            assert_eq!(
                page.len() % value_size,
                0,
                "dirty page length not a multiple of value_size"
            );

            // Reposition and read again; must be consistent.
            cursor.first::<(), ()>().unwrap();
            let second: Option<Vec<u8>> = cursor.get_multiple().unwrap();
            assert_eq!(
                page.as_slice(),
                second.unwrap().as_slice(),
                "inconsistent get_multiple reads"
            );

            page.len()
        };

        txn.commit().unwrap();
        (dirty_len, inserted.len())
    };

    // Read via RO transaction (clean page) and verify consistency.
    let ro_txn = env.begin_ro_unsync().unwrap();
    let ro_db = ro_txn.open_db(None).unwrap();
    let mut ro_cursor = ro_txn.cursor(ro_db).unwrap();
    ro_cursor.first::<(), ()>().unwrap();
    let clean: Option<Vec<u8>> = ro_cursor.get_multiple().unwrap();

    let clean_len = clean.map(|p| {
        assert_eq!(p.len() % value_size, 0, "clean page length not a multiple of value_size");
        p.len()
    });

    // Total items returned must match what we inserted (may span multiple
    // pages; get_multiple only returns up to one page, so just verify
    // divisibility and non-zero length).
    assert!(clean_len.unwrap_or(0) > 0 || inserted_count == 0);
    assert_eq!(dirty_len, clean_len.unwrap_or(0), "dirty vs clean page byte counts differ");

    // Exercise IterDupFixedOfKey to test page-splitting logic.
    let mut iter_cursor = ro_txn.cursor(ro_db).unwrap();
    let iter = iter_cursor.iter_dupfixed_of::<Vec<u8>>(b"key").unwrap();
    let iter_values: Vec<Vec<u8>> = iter.map(Result::unwrap).collect();
    assert_eq!(iter_values.len(), inserted_count, "iterator count mismatch");
    for val in &iter_values {
        assert_eq!(val.len(), value_size, "iterator yielded wrong-sized value");
    }
});
