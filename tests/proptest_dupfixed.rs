//! Property-based tests for DUP_FIXED operations.
//!
//! Tests focus on both "does not panic" and correctness properties. Errors are
//! acceptable (e.g., `BadValSize`), panics are not.
#![allow(missing_docs)]

use proptest::prelude::*;
use signet_libmdbx::{DatabaseFlags, Environment, Geometry, WriteFlags};
use tempfile::tempdir;

// =============================================================================
// Roundtrip: 8-byte values
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Test that put/iter_dupfixed_of roundtrips 8-byte values correctly (V2).
    #[test]
    fn dupfixed_roundtrip_8(
        n_values in 1usize..20,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        // Insert n_values distinct 8-byte values under a single key.
        let mut expected: Vec<[u8; 8]> = (0..n_values)
            .map(|i| {
                let mut v = [0u8; 8];
                v[0] = i as u8;
                v[1] = (i >> 8) as u8;
                v
            })
            .collect();

        for value in &expected {
            txn.put(db, b"key", value.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<[u8; 8]> = cursor
            .iter_dupfixed_of::<[u8; 8]>(b"key")
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        expected.sort();
        let mut retrieved_sorted = retrieved;
        retrieved_sorted.sort();
        prop_assert_eq!(retrieved_sorted, expected);
    }

    /// Test that put/iter_dupfixed_of roundtrips 32-byte values correctly (V2).
    #[test]
    fn dupfixed_roundtrip_32(
        n_values in 1usize..20,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        let mut expected: Vec<[u8; 32]> = (0..n_values)
            .map(|i| {
                let mut v = [0u8; 32];
                v[0] = i as u8;
                v[1] = (i >> 8) as u8;
                v
            })
            .collect();

        for value in &expected {
            txn.put(db, b"key", value.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<[u8; 32]> = cursor
            .iter_dupfixed_of::<[u8; 32]>(b"key")
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        expected.sort();
        let mut retrieved_sorted = retrieved;
        retrieved_sorted.sort();
        prop_assert_eq!(retrieved_sorted, expected);
    }

    /// Test that put/iter_dupfixed_of roundtrips 100-byte values correctly (V2).
    #[test]
    fn dupfixed_roundtrip_100(
        n_values in 1usize..20,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        let mut expected: Vec<[u8; 100]> = (0..n_values)
            .map(|i| {
                let mut v = [0u8; 100];
                v[0] = i as u8;
                v[1] = (i >> 8) as u8;
                v
            })
            .collect();

        for value in &expected {
            txn.put(db, b"key", value.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<[u8; 100]> = cursor
            .iter_dupfixed_of::<[u8; 100]>(b"key")
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        expected.sort();
        let mut retrieved_sorted = retrieved;
        retrieved_sorted.sort();
        prop_assert_eq!(retrieved_sorted, expected);
    }
}

// =============================================================================
// Roundtrip (sync transactions): 8-byte and 32-byte values
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Test that put/iter_dupfixed_of roundtrips 8-byte values correctly (sync).
    #[test]
    fn dupfixed_roundtrip_8_sync(
        n_values in 1usize..20,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        let mut expected: Vec<[u8; 8]> = (0..n_values)
            .map(|i| {
                let mut v = [0u8; 8];
                v[0] = i as u8;
                v[1] = (i >> 8) as u8;
                v
            })
            .collect();

        for value in &expected {
            txn.put(db, b"key", value.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<[u8; 8]> = cursor
            .iter_dupfixed_of::<[u8; 8]>(b"key")
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        expected.sort();
        let mut retrieved_sorted = retrieved;
        retrieved_sorted.sort();
        prop_assert_eq!(retrieved_sorted, expected);
    }

    /// Test that put/iter_dupfixed_of roundtrips 32-byte values correctly (sync).
    #[test]
    fn dupfixed_roundtrip_32_sync(
        n_values in 1usize..20,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        let mut expected: Vec<[u8; 32]> = (0..n_values)
            .map(|i| {
                let mut v = [0u8; 32];
                v[0] = i as u8;
                v[1] = (i >> 8) as u8;
                v
            })
            .collect();

        for value in &expected {
            txn.put(db, b"key", value.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<[u8; 32]> = cursor
            .iter_dupfixed_of::<[u8; 32]>(b"key")
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        expected.sort();
        let mut retrieved_sorted = retrieved;
        retrieved_sorted.sort();
        prop_assert_eq!(retrieved_sorted, expected);
    }
}

// =============================================================================
// Completeness: iter_dupfixed_start
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Test that iter_dupfixed_start yields exactly N items inserted under one key (V2).
    #[test]
    fn iter_dupfixed_start_completeness(
        n_values in 1usize..100,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        // Insert n_values distinct 8-byte values under b"key" (3 bytes).
        for i in 0..n_values {
            let mut v = [0u8; 8];
            v[0] = i as u8;
            v[1] = (i >> 8) as u8;
            txn.put(db, b"key", v.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        // Key type is [u8; 3] since "key" is 3 bytes, value type is [u8; 8].
        let count = cursor
            .iter_dupfixed_start::<[u8; 3], [u8; 8]>()
            .unwrap()
            .filter_map(Result::ok)
            .count();

        prop_assert_eq!(count, n_values);
    }
}

// =============================================================================
// Page spanning: large numbers of fixed-size values
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// Test that all 64-byte values survive across page boundaries (V2).
    #[test]
    fn dupfixed_page_spanning(
        n_values in 100usize..500,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_geometry(Geometry { size: Some(0..(64 * 1024 * 1024)), ..Default::default() })
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        // Insert n_values distinct 64-byte values under a single key.
        for i in 0..n_values {
            let mut v = [0u8; 64];
            v[0] = i as u8;
            v[1] = (i >> 8) as u8;
            txn.put(db, b"key", v.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<[u8; 64]> = cursor
            .iter_dupfixed_of::<[u8; 64]>(b"key")
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        prop_assert_eq!(retrieved.len(), n_values);
    }

    /// Test that all 64-byte values survive across page boundaries (sync).
    #[test]
    fn dupfixed_page_spanning_sync(
        n_values in 100usize..500,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_geometry(Geometry { size: Some(0..(64 * 1024 * 1024)), ..Default::default() })
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED).unwrap();

        for i in 0..n_values {
            let mut v = [0u8; 64];
            v[0] = i as u8;
            v[1] = (i >> 8) as u8;
            txn.put(db, b"key", v.as_slice(), WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<[u8; 64]> = cursor
            .iter_dupfixed_of::<[u8; 64]>(b"key")
            .unwrap()
            .filter_map(Result::ok)
            .collect();

        prop_assert_eq!(retrieved.len(), n_values);
    }
}
