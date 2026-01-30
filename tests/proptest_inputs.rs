//! Property-based tests to ensure arbitrary inputs do not cause panics.
//!
//! These tests focus on "does not panic" rather than correctness. Errors are
//! acceptable (e.g., `BadValSize`), panics are not.
#![allow(missing_docs)]

use proptest::prelude::*;
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use tempfile::tempdir;

/// Strategy for generating byte vectors of various sizes (0 to 1KB).
fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..1024)
}

/// Strategy for generating small byte vectors (0 to 64 bytes).
fn arb_small_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..64)
}

/// Strategy for valid database names (alphanumeric + underscore, 1-64 chars).
fn arb_db_name() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,63}"
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
}

// =============================================================================
// Cursor Operations
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that cursor.set() with arbitrary key does not panic (V1).
    #[test]
    fn cursor_set_arbitrary_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Add some data so cursor is positioned
        txn.put(db, b"test_key", b"test_val", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        // set() with arbitrary key should return None or value, never panic
        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> = cursor.set(&key);
        prop_assert!(result.is_ok());
    }

    /// Test that cursor.set_range() with arbitrary key does not panic (V1).
    #[test]
    fn cursor_set_range_arbitrary_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Add some data
        txn.put(db, b"aaa", b"val_a", WriteFlags::empty()).unwrap();
        txn.put(db, b"zzz", b"val_z", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        // set_range() with arbitrary key should not panic
        let result: signet_libmdbx::ReadResult<Option<(Vec<u8>, Vec<u8>)>> =
            cursor.set_range(&key);
        prop_assert!(result.is_ok());
    }

    /// Test that cursor.set_key() with arbitrary key does not panic (V1).
    #[test]
    fn cursor_set_key_arbitrary_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db, b"test", b"value", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        // set_key() should not panic
        let result: signet_libmdbx::ReadResult<Option<(Vec<u8>, Vec<u8>)>> =
            cursor.set_key(&key);
        prop_assert!(result.is_ok());
    }

    /// Test that cursor.set() with arbitrary key does not panic (V2).
    #[test]
    fn cursor_set_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db, b"test_key", b"test_val", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> = cursor.set(&key);
        prop_assert!(result.is_ok());
    }

    /// Test that cursor.set_range() with arbitrary key does not panic (V2).
    #[test]
    fn cursor_set_range_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db, b"aaa", b"val_a", WriteFlags::empty()).unwrap();
        txn.put(db, b"zzz", b"val_z", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        let result: signet_libmdbx::ReadResult<Option<(Vec<u8>, Vec<u8>)>> =
            cursor.set_range(&key);
        prop_assert!(result.is_ok());
    }

    /// Test that cursor.set_key() with arbitrary key does not panic (V2).
    #[test]
    fn cursor_set_key_arbitrary_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db, b"test", b"value", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        let result: signet_libmdbx::ReadResult<Option<(Vec<u8>, Vec<u8>)>> =
            cursor.set_key(&key);
        prop_assert!(result.is_ok());
    }
}

// =============================================================================
// Database Names
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that create_db with arbitrary valid names does not panic (V1).
    #[test]
    fn create_db_arbitrary_name_v1(name in arb_db_name()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_max_dbs(16)
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_sync().unwrap();

        // create_db should not panic, may return error for invalid names
        let result = txn.create_db(Some(&name), DatabaseFlags::empty());
        // We accept both success and error, just no panic
        let _ = result;
    }

    /// Test that create_db with arbitrary valid names does not panic (V2).
    #[test]
    fn create_db_arbitrary_name_v2(name in arb_db_name()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder()
            .set_max_dbs(16)
            .open(dir.path())
            .unwrap();
        let txn = env.begin_rw_unsync().unwrap();

        let result = txn.create_db(Some(&name), DatabaseFlags::empty());
        let _ = result;
    }
}

// =============================================================================
// DUP_SORT Operations
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that DUP_SORT put with multiple values does not panic (V1).
    #[test]
    fn dupsort_put_multiple_values_v1(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 1..10),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for value in &values {
            // Should not panic
            let result = txn.put(db, &key, value, WriteFlags::empty());
            // Errors are acceptable, panics are not
            let _ = result;
        }
    }

    /// Test that DUP_SORT put with multiple values does not panic (V2).
    #[test]
    fn dupsort_put_multiple_values_v2(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 1..10),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for value in &values {
            let result = txn.put(db, &key, value, WriteFlags::empty());
            let _ = result;
        }
    }

    /// Test cursor get_both with arbitrary key/value does not panic (V1).
    #[test]
    fn cursor_get_both_arbitrary_v1(
        search_key in arb_small_bytes(),
        search_value in arb_small_bytes(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Add some data
        txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        // get_both should not panic
        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            cursor.get_both(&search_key, &search_value);
        prop_assert!(result.is_ok());
    }

    /// Test cursor get_both_range with arbitrary key/value does not panic (V1).
    #[test]
    fn cursor_get_both_range_arbitrary_v1(
        search_key in arb_small_bytes(),
        search_value in arb_small_bytes(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        // get_both_range should not panic
        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            cursor.get_both_range(&search_key, &search_value);
        prop_assert!(result.is_ok());
    }

    /// Test cursor get_both with arbitrary key/value does not panic (V2).
    #[test]
    fn cursor_get_both_arbitrary_v2(
        search_key in arb_small_bytes(),
        search_value in arb_small_bytes(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            cursor.get_both(&search_key, &search_value);
        prop_assert!(result.is_ok());
    }

    /// Test cursor get_both_range with arbitrary key/value does not panic (V2).
    #[test]
    fn cursor_get_both_range_arbitrary_v2(
        search_key in arb_small_bytes(),
        search_value in arb_small_bytes(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        txn.put(db, b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db, b"key1", b"val2", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            cursor.get_both_range(&search_key, &search_value);
        prop_assert!(result.is_ok());
    }
}

// =============================================================================
// Iterator Operations
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

        // Add some data
        for i in 0u8..10 {
            txn.put(db, [i], [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        // iter_from should not panic
        let result = cursor.iter_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        // Consuming the iterator should not panic
        let count = result.unwrap().count();
        prop_assert!(count <= 10);
    }

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

    /// Test iter_dup_of with arbitrary key does not panic (V1).
    #[test]
    fn iter_dup_of_arbitrary_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Add some dup data
        for i in 0u8..5 {
            txn.put(db, b"known_key", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        // iter_dup_of should not panic (yields just values, not (key, value))
        let result = cursor.iter_dup_of::<Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        // Consuming the iterator should not panic
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

        // iter_dup_from should not panic (now yields flat (key, value) pairs)
        let result = cursor.iter_dup_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        // Consuming iterator should not panic (no nested iteration anymore)
        let _ = result.unwrap().count();
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

        // iter_dup_of yields just values, not (key, value)
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

        // iter_dup_from now yields flat (key, value) pairs
        let result = cursor.iter_dup_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        // No nested iteration anymore - just count the items
        let _ = result.unwrap().count();
    }
}

// =============================================================================
// Cursor Put Operations
// =============================================================================

/// Strategy for keys that won't trigger MDBX assertion failures.
/// MDBX max key size is ~2022 bytes for 4KB pages.
fn arb_safe_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..512)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test cursor.put with arbitrary key/value does not panic (V1).
    ///
    /// Note: Uses constrained key sizes because MDBX aborts on very large keys
    /// via cursor.put (assertion failure in cursor_put_checklen).
    #[test]
    fn cursor_put_arbitrary_v1(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        // cursor.put should not panic
        let result = cursor.put(&key, &value, WriteFlags::empty());
        // Errors are fine (e.g., BadValSize), panics are not
        let _ = result;
    }

    /// Test cursor.put with arbitrary key/value does not panic (V2).
    #[test]
    fn cursor_put_arbitrary_v2(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.put(&key, &value, WriteFlags::empty());
        let _ = result;
    }
}

// =============================================================================
// Edge Cases
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

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
// Correctness: DUP_SORT Values - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that all unique DUP_SORT values are retrievable via iter_dup_of (V1).
    #[test]
    fn dupsort_values_correctness_v1(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 1..10),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Insert all values
        let mut inserted: Vec<Vec<u8>> = Vec::new();
        for value in &values {
            if txn.put(db, &key, value, WriteFlags::empty()).is_ok()
                && !inserted.contains(value)
            {
                inserted.push(value.clone());
            }
        }

        // Skip if nothing was inserted
        prop_assume!(!inserted.is_empty());

        // Retrieve all values via iter_dup_of (yields just values, not (key, value))
        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<Vec<u8>> =
            cursor.iter_dup_of::<Vec<u8>>(&key).unwrap().filter_map(Result::ok).collect();

        // All inserted values should be retrieved (order is sorted by MDBX)
        inserted.sort();
        let mut retrieved_sorted = retrieved.clone();
        retrieved_sorted.sort();
        prop_assert_eq!(inserted, retrieved_sorted);
    }
}

// =============================================================================
// Correctness: DUP_SORT Values - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that all unique DUP_SORT values are retrievable via iter_dup_of (V2).
    #[test]
    fn dupsort_values_correctness_v2(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 1..10),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        let mut inserted: Vec<Vec<u8>> = Vec::new();
        for value in &values {
            if txn.put(db, &key, value, WriteFlags::empty()).is_ok()
                && !inserted.contains(value)
            {
                inserted.push(value.clone());
            }
        }

        prop_assume!(!inserted.is_empty());

        // iter_dup_of yields just values, not (key, value)
        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<Vec<u8>> =
            cursor.iter_dup_of::<Vec<u8>>(&key).unwrap().filter_map(Result::ok).collect();

        inserted.sort();
        let mut retrieved_sorted = retrieved.clone();
        retrieved_sorted.sort();
        prop_assert_eq!(inserted, retrieved_sorted);
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
// Correctness: Cursor Set - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that cursor.set returns the correct value when key exists (V1).
    #[test]
    fn cursor_set_correctness_v1(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let mut cursor = txn.cursor(db).unwrap();
            let retrieved: Option<Vec<u8>> = cursor.set(&key).unwrap();
            prop_assert_eq!(retrieved, Some(value));
        }
    }
}

// =============================================================================
// Correctness: Cursor Set - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Test that cursor.set returns the correct value when key exists (V2).
    #[test]
    fn cursor_set_correctness_v2(key in arb_safe_key(), value in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db, &key, &value, WriteFlags::empty());
        if put_result.is_ok() {
            let mut cursor = txn.cursor(db).unwrap();
            let retrieved: Option<Vec<u8>> = cursor.set(&key).unwrap();
            prop_assert_eq!(retrieved, Some(value));
        }
    }
}

// =============================================================================
// Correctness: Cursor Set Range - TxSync (V1)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that cursor.set_range returns the first key >= search key (V1).
    #[test]
    fn cursor_set_range_correctness_v1(
        entries in prop::collection::vec((arb_safe_key(), arb_bytes()), 2..10),
        search_key in arb_safe_key(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut inserted: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (key, value) in &entries {
            if txn.put(db, key, value, WriteFlags::empty()).is_ok() {
                inserted.push((key.clone(), value.clone()));
            }
        }

        prop_assume!(!inserted.is_empty());

        // Sort by key to find expected result
        inserted.sort_by(|a, b| a.0.cmp(&b.0));
        inserted.dedup_by(|a, b| a.0 == b.0);

        let expected = inserted
            .iter()
            .find(|(k, _)| k >= &search_key)
            .cloned();

        let mut cursor = txn.cursor(db).unwrap();
        let result: Option<(Vec<u8>, Vec<u8>)> = cursor.set_range(&search_key).unwrap();

        prop_assert_eq!(result, expected);
    }
}

// =============================================================================
// Correctness: Cursor Set Range - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that cursor.set_range returns the first key >= search key (V2).
    #[test]
    fn cursor_set_range_correctness_v2(
        entries in prop::collection::vec((arb_safe_key(), arb_bytes()), 2..10),
        search_key in arb_safe_key(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut inserted: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (key, value) in &entries {
            if txn.put(db, key, value, WriteFlags::empty()).is_ok() {
                inserted.push((key.clone(), value.clone()));
            }
        }

        prop_assume!(!inserted.is_empty());

        inserted.sort_by(|a, b| a.0.cmp(&b.0));
        inserted.dedup_by(|a, b| a.0 == b.0);

        let expected = inserted
            .iter()
            .find(|(k, _)| k >= &search_key)
            .cloned();

        let mut cursor = txn.cursor(db).unwrap();
        let result: Option<(Vec<u8>, Vec<u8>)> = cursor.set_range(&search_key).unwrap();

        prop_assert_eq!(result, expected);
    }
}
