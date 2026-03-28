//! Property-based tests for iterator operations.
//!
//! Tests focus on both "does not panic" and correctness properties. Errors are
//! acceptable (e.g., `BadValSize`), panics are not.
#![allow(missing_docs)]

use proptest::prelude::*;
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use tempfile::tempdir;

/// Strategy for generating byte vectors of various sizes (0 to 1KB).
fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..1024)
}

/// Strategy for keys that won't trigger MDBX assertion failures.
/// MDBX max key size is ~2022 bytes for 4KB pages.
fn arb_safe_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..512)
}

// =============================================================================
// Migrated: iter_from — TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test iter_from with arbitrary key does not panic (V1).
    #[test]
    fn iter_from_arbitrary_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        for i in 0u8..10 {
            txn.put(db, [i], [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        let count = result.unwrap().count();
        prop_assert!(count <= 10);
    }

    /// Test iter_dup_of with arbitrary key does not panic (V1).
    #[test]
    fn iter_dup_of_arbitrary_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for i in 0u8..5 {
            txn.put(db, b"known_key", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_dup_of::<Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        let _ = result.unwrap().count();
    }

    /// Test iter_dup_from with arbitrary key does not panic (V1).
    #[test]
    fn iter_dup_from_arbitrary_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for i in 0u8..5 {
            txn.put(db, b"key_a", [i], WriteFlags::empty()).unwrap();
            txn.put(db, b"key_z", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_dup_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        let _ = result.unwrap().count();
    }
}

// =============================================================================
// Migrated: iter_from — TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test iter_from with arbitrary key does not panic (V2).
    #[test]
    fn iter_from_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        for i in 0u8..10 {
            txn.put(db, [i], [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        let count = result.unwrap().count();
        prop_assert!(count <= 10);
    }

    /// Test iter_dup_of with arbitrary key does not panic (V2).
    #[test]
    fn iter_dup_of_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for i in 0u8..5 {
            txn.put(db, b"known_key", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_dup_of::<Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        let _ = result.unwrap().count();
    }

    /// Test iter_dup_from with arbitrary key does not panic (V2).
    #[test]
    fn iter_dup_from_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for i in 0u8..5 {
            txn.put(db, b"key_a", [i], WriteFlags::empty()).unwrap();
            txn.put(db, b"key_z", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_dup_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        let _ = result.unwrap().count();
    }
}

// =============================================================================
// New: iter_start yields all — TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that iter_start yields exactly the number of distinct keys inserted (V1).
    #[test]
    fn iter_start_yields_all_v1(
        keys in prop::collection::vec(arb_safe_key(), 1..20),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Deduplicate and sort keys, then insert all.
        let mut unique_keys = keys;
        unique_keys.sort();
        unique_keys.dedup();

        for key in &unique_keys {
            txn.put(db, key, b"v", WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let count = cursor
            .iter_start::<Vec<u8>, Vec<u8>>()
            .unwrap()
            .filter_map(Result::ok)
            .count();

        prop_assert_eq!(count, unique_keys.len());
    }
}

// =============================================================================
// New: iter_start yields all — TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that iter_start yields exactly the number of distinct keys inserted (V2).
    #[test]
    fn iter_start_yields_all_v2(
        keys in prop::collection::vec(arb_safe_key(), 1..20),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut unique_keys = keys;
        unique_keys.sort();
        unique_keys.dedup();

        for key in &unique_keys {
            txn.put(db, key, b"v", WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();
        let count = cursor
            .iter_start::<Vec<u8>, Vec<u8>>()
            .unwrap()
            .filter_map(Result::ok)
            .count();

        prop_assert_eq!(count, unique_keys.len());
    }
}

// =============================================================================
// New: iter_from bounds — TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that iter_from returns only keys >= search key (V1).
    #[test]
    fn iter_from_bounds_v1(search_idx in 0usize..20) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Insert 20 single-byte keys [0]...[19].
        for i in 0u8..20 {
            txn.put(db, [i], b"v", WriteFlags::empty()).unwrap();
        }

        let search_key = [search_idx as u8];
        let mut cursor = txn.cursor(db).unwrap();
        let retrieved_keys: Vec<Vec<u8>> = cursor
            .iter_from::<Vec<u8>, Vec<u8>>(&search_key)
            .unwrap()
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();

        // All returned keys must be >= search_key.
        for k in &retrieved_keys {
            prop_assert!(k.as_slice() >= search_key.as_slice());
        }

        // The number of returned keys should be 20 - search_idx.
        prop_assert_eq!(retrieved_keys.len(), 20 - search_idx);
    }
}

// =============================================================================
// New: iter_from bounds — TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that iter_from returns only keys >= search key (V2).
    #[test]
    fn iter_from_bounds_v2(search_idx in 0usize..20) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        for i in 0u8..20 {
            txn.put(db, [i], b"v", WriteFlags::empty()).unwrap();
        }

        let search_key = [search_idx as u8];
        let mut cursor = txn.cursor(db).unwrap();
        let retrieved_keys: Vec<Vec<u8>> = cursor
            .iter_from::<Vec<u8>, Vec<u8>>(&search_key)
            .unwrap()
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();

        for k in &retrieved_keys {
            prop_assert!(k.as_slice() >= search_key.as_slice());
        }

        prop_assert_eq!(retrieved_keys.len(), 20 - search_idx);
    }
}
