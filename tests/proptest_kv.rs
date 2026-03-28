//! Property-based tests for key/value operations.
//!
//! Tests focus on both "does not panic" and correctness properties. Errors are
//! acceptable (e.g., `BadValSize`), panics are not.
#![allow(missing_docs)]

use proptest::prelude::*;
use signet_libmdbx::{Environment, Geometry, WriteFlags};
use tempfile::tempdir;

/// Strategy for generating byte vectors of various sizes (0 to 1KB).
fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..1024)
}

/// Strategy for generating small byte vectors (0 to 64 bytes).
fn arb_small_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..64)
}

/// Strategy for keys that won't trigger MDBX assertion failures.
/// MDBX max key size is ~2022 bytes for 4KB pages.
fn arb_safe_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..512)
}

// =============================================================================
// Key/Value Operations - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that put/get with arbitrary key/value does not panic (V1).
    #[test]
    fn put_get_arbitrary_kv_v1(key in arb_bytes(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Should not panic - may return error for invalid sizes
        let put_result = txn.put(db, &key, &value, WriteFlags::empty());

        // If put succeeded, get should not panic
        if put_result.is_ok() {
            let _: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
        }
    }

    /// Test that del with nonexistent arbitrary key does not panic (V1).
    #[test]
    fn del_nonexistent_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Delete on nonexistent key should return Ok(false), not panic
        let result = txn.del(db, &key, None);
        prop_assert!(result.is_ok());
        prop_assert!(!result.unwrap());
    }

    /// Test that get with arbitrary key on empty db does not panic (V1).
    #[test]
    fn get_arbitrary_key_empty_db_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_ro_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Get on nonexistent key should return Ok(None), not panic
        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> = txn.get(db.dbi(), &key);
        prop_assert!(result.is_ok());
        prop_assert!(result.unwrap().is_none());
    }

    /// Test empty key handling does not panic (V1).
    #[test]
    fn empty_key_operations_v1(value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Empty key should be valid
        let put_result = txn.put(db, b"", &value, WriteFlags::empty());
        prop_assert!(put_result.is_ok());

        let get_result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            txn.get(db.dbi(), b"");
        prop_assert!(get_result.is_ok());

        let del_result = txn.del(db, b"", None);
        prop_assert!(del_result.is_ok());
    }

    /// Test empty value handling does not panic (V1).
    #[test]
    fn empty_value_operations_v1(key in arb_small_bytes()) {
        // Skip empty keys for this test
        prop_assume!(!key.is_empty());

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Empty value should be valid
        let put_result = txn.put(db, &key, b"", WriteFlags::empty());
        prop_assert!(put_result.is_ok());

        let get_result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            txn.get(db.dbi(), &key);
        prop_assert!(get_result.is_ok());
        prop_assert!(get_result.unwrap().is_some());
    }
}

// =============================================================================
// Key/Value Operations - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that put/get with arbitrary key/value does not panic (V2).
    #[test]
    fn put_get_arbitrary_kv_v2(key in arb_bytes(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Should not panic - may return error for invalid sizes
        let put_result = txn.put(db, &key, &value, WriteFlags::empty());

        // If put succeeded, get should not panic
        if put_result.is_ok() {
            let _: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
        }
    }

    /// Test that del with nonexistent arbitrary key does not panic (V2).
    #[test]
    fn del_nonexistent_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Delete on nonexistent key should return Ok(false), not panic
        let result = txn.del(db, &key, None);
        prop_assert!(result.is_ok());
        prop_assert!(!result.unwrap());
    }

    /// Test that get with arbitrary key on empty db does not panic (V2).
    #[test]
    fn get_arbitrary_key_empty_db_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_ro_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Get on nonexistent key should return Ok(None), not panic
        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> = txn.get(db.dbi(), &key);
        prop_assert!(result.is_ok());
        prop_assert!(result.unwrap().is_none());
    }

    /// Test empty key handling does not panic (V2).
    #[test]
    fn empty_key_operations_v2(value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, b"", &value, WriteFlags::empty());
        prop_assert!(put_result.is_ok());

        let get_result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            txn.get(db.dbi(), b"");
        prop_assert!(get_result.is_ok());

        let del_result = txn.del(db, b"", None);
        prop_assert!(del_result.is_ok());
    }

    /// Test empty value handling does not panic (V2).
    #[test]
    fn empty_value_operations_v2(key in arb_small_bytes()) {
        prop_assume!(!key.is_empty());

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, b"", WriteFlags::empty());
        prop_assert!(put_result.is_ok());

        let get_result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            txn.get(db.dbi(), &key);
        prop_assert!(get_result.is_ok());
        prop_assert!(get_result.unwrap().is_some());
    }
}

// =============================================================================
// Correctness: Round-trip - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that put followed by get returns the same value (V1).
    #[test]
    fn roundtrip_correctness_v1(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, Some(value));
        }
    }
}

// =============================================================================
// Correctness: Round-trip - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that put followed by get returns the same value (V2).
    #[test]
    fn roundtrip_correctness_v2(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, Some(value));
        }
    }
}

// =============================================================================
// Correctness: Overwrite - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that overwriting a key returns the new value (V1).
    #[test]
    fn overwrite_correctness_v1(
        key in arb_safe_key(),
        value1 in arb_bytes(),
        value2 in arb_bytes(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put1 = txn.put(db, &key, &value1, WriteFlags::empty());
        let put2 = txn.put(db, &key, &value2, WriteFlags::empty());

        if put1.is_ok() && put2.is_ok() {
            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, Some(value2));
        }
    }
}

// =============================================================================
// Correctness: Overwrite - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that overwriting a key returns the new value (V2).
    #[test]
    fn overwrite_correctness_v2(
        key in arb_safe_key(),
        value1 in arb_bytes(),
        value2 in arb_bytes(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put1 = txn.put(db, &key, &value1, WriteFlags::empty());
        let put2 = txn.put(db, &key, &value2, WriteFlags::empty());

        if put1.is_ok() && put2.is_ok() {
            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, Some(value2));
        }
    }
}

// =============================================================================
// Correctness: Delete - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that delete removes the key and get returns None (V1).
    #[test]
    fn delete_correctness_v1(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let deleted = txn.del(db, &key, None).unwrap();
            prop_assert!(deleted);

            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, None);
        }
    }
}

// =============================================================================
// Correctness: Delete - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that delete removes the key and get returns None (V2).
    #[test]
    fn delete_correctness_v2(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let deleted = txn.del(db, &key, None).unwrap();
            prop_assert!(deleted);

            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, None);
        }
    }
}

// =============================================================================
// Correctness: Iteration Order - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that keys are returned in lexicographically sorted order (V1).
    #[test]
    fn iteration_order_correctness_v1(
        entries in prop::collection::vec((arb_safe_key(), arb_bytes()), 1..20),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Insert all entries
        let mut inserted_keys: Vec<Vec<u8>> = Vec::new();
        for (key, value) in &entries {
            if txn.put(db, key, value, WriteFlags::empty()).is_ok()
                && !inserted_keys.contains(key)
            {
                inserted_keys.push(key.clone());
            }
        }

        prop_assume!(!inserted_keys.is_empty());

        // Iterate and collect keys
        let mut cursor = txn.cursor(db).unwrap();
        let retrieved_keys: Vec<Vec<u8>> = cursor
            .iter::<Vec<u8>, Vec<u8>>()
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();

        // Keys should be in sorted order
        let mut expected = inserted_keys;
        expected.sort();
        expected.dedup();
        prop_assert_eq!(retrieved_keys, expected);
    }
}

// =============================================================================
// Correctness: Iteration Order - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that keys are returned in lexicographically sorted order (V2).
    #[test]
    fn iteration_order_correctness_v2(
        entries in prop::collection::vec((arb_safe_key(), arb_bytes()), 1..20),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut inserted_keys: Vec<Vec<u8>> = Vec::new();
        for (key, value) in &entries {
            if txn.put(db, key, value, WriteFlags::empty()).is_ok()
                && !inserted_keys.contains(key)
            {
                inserted_keys.push(key.clone());
            }
        }

        prop_assume!(!inserted_keys.is_empty());

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved_keys: Vec<Vec<u8>> = cursor
            .iter::<Vec<u8>, Vec<u8>>()
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();

        let mut expected = inserted_keys;
        expected.sort();
        expected.dedup();
        prop_assert_eq!(retrieved_keys, expected);
    }
}

// =============================================================================
// New: Large Value Roundtrip
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Test roundtrip with large values (up to 64KB) using a larger environment (V1).
    #[test]
    fn large_value_roundtrip_v1(
        key in arb_safe_key(),
        value in prop::collection::vec(any::<u8>(), 0..65536),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_geometry(Geometry { size: Some(0..(256 * 1024 * 1024)), ..Default::default() })
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, Some(value));
        }
    }

    /// Test roundtrip with large values (up to 64KB) using a larger environment (V2).
    #[test]
    fn large_value_roundtrip_v2(
        key in arb_safe_key(),
        value in prop::collection::vec(any::<u8>(), 0..65536),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_geometry(Geometry { size: Some(0..(256 * 1024 * 1024)), ..Default::default() })
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved, Some(value));
        }
    }
}

// =============================================================================
// New: Multi-Database Isolation
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that two named databases are isolated from each other (V1).
    #[test]
    fn multi_database_isolation_v1(
        key in arb_safe_key(),
        value_a in arb_bytes(),
        value_b in arb_bytes(),
    ) {
        // Values must differ for isolation check to be meaningful
        prop_assume!(value_a != value_b);

        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_max_dbs(4)
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db_a = txn.create_db(Some("db_a"), signet_libmdbx::DatabaseFlags::empty()).unwrap();
        let db_b = txn.create_db(Some("db_b"), signet_libmdbx::DatabaseFlags::empty()).unwrap();

        let put_a = txn.put(db_a, &key, &value_a, WriteFlags::empty());
        let put_b = txn.put(db_b, &key, &value_b, WriteFlags::empty());

        if put_a.is_ok() && put_b.is_ok() {
            let retrieved_a: Option<Vec<u8>> = txn.get(db_a.dbi(), &key).unwrap();
            let retrieved_b: Option<Vec<u8>> = txn.get(db_b.dbi(), &key).unwrap();
            // Each db should return its own value, not the other's
            prop_assert_eq!(retrieved_a, Some(value_a));
            prop_assert_eq!(retrieved_b, Some(value_b));
        }
    }

    /// Test that two named databases are isolated from each other (V2).
    #[test]
    fn multi_database_isolation_v2(
        key in arb_safe_key(),
        value_a in arb_bytes(),
        value_b in arb_bytes(),
    ) {
        prop_assume!(value_a != value_b);

        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_max_dbs(4)
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db_a = txn.create_db(Some("db_a"), signet_libmdbx::DatabaseFlags::empty()).unwrap();
        let db_b = txn.create_db(Some("db_b"), signet_libmdbx::DatabaseFlags::empty()).unwrap();

        let put_a = txn.put(db_a, &key, &value_a, WriteFlags::empty());
        let put_b = txn.put(db_b, &key, &value_b, WriteFlags::empty());

        if put_a.is_ok() && put_b.is_ok() {
            let retrieved_a: Option<Vec<u8>> = txn.get(db_a.dbi(), &key).unwrap();
            let retrieved_b: Option<Vec<u8>> = txn.get(db_b.dbi(), &key).unwrap();
            prop_assert_eq!(retrieved_a, Some(value_a));
            prop_assert_eq!(retrieved_b, Some(value_b));
        }
    }
}
