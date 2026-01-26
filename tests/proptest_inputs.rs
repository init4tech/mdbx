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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Should not panic - may return error for invalid sizes
        let put_result = txn.put(db.dbi(), &key, &value, WriteFlags::empty());

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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Delete on nonexistent key should return Ok(false), not panic
        let result = txn.del(db.dbi(), &key, None);
        prop_assert!(result.is_ok());
        prop_assert!(!result.unwrap());
    }

    /// Test that get with arbitrary key on empty db does not panic (V1).
    #[test]
    fn get_arbitrary_key_empty_db_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_ro_txn().unwrap();
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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Should not panic - may return error for invalid sizes
        let put_result = txn.put(db.dbi(), &key, &value, WriteFlags::empty());

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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Delete on nonexistent key should return Ok(false), not panic
        let result = txn.del(db.dbi(), &key, None);
        prop_assert!(result.is_ok());
        prop_assert!(!result.unwrap());
    }

    /// Test that get with arbitrary key on empty db does not panic (V2).
    #[test]
    fn get_arbitrary_key_empty_db_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let mut txn = env.begin_ro_unsync().unwrap();
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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Add some data so cursor is positioned
        txn.put(db.dbi(), b"test_key", b"test_val", WriteFlags::empty()).unwrap();

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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Add some data
        txn.put(db.dbi(), b"aaa", b"val_a", WriteFlags::empty()).unwrap();
        txn.put(db.dbi(), b"zzz", b"val_z", WriteFlags::empty()).unwrap();

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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db.dbi(), b"test", b"value", WriteFlags::empty()).unwrap();

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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db.dbi(), b"test_key", b"test_val", WriteFlags::empty()).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        let result: signet_libmdbx::ReadResult<Option<Vec<u8>>> = cursor.set(&key);
        prop_assert!(result.is_ok());
    }

    /// Test that cursor.set_range() with arbitrary key does not panic (V2).
    #[test]
    fn cursor_set_range_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db.dbi(), b"aaa", b"val_a", WriteFlags::empty()).unwrap();
        txn.put(db.dbi(), b"zzz", b"val_z", WriteFlags::empty()).unwrap();

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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        txn.put(db.dbi(), b"test", b"value", WriteFlags::empty()).unwrap();

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
        let txn = env.begin_rw_txn().unwrap();

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
        let mut txn = env.begin_rw_unsync().unwrap();

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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for value in &values {
            // Should not panic
            let result = txn.put(db.dbi(), &key, value, WriteFlags::empty());
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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for value in &values {
            let result = txn.put(db.dbi(), &key, value, WriteFlags::empty());
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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Add some data
        txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();

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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();

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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();

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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty()).unwrap();
        txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty()).unwrap();

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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Add some data
        for i in 0u8..10 {
            txn.put(db.dbi(), [i], [i], WriteFlags::empty()).unwrap();
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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        for i in 0u8..10 {
            txn.put(db.dbi(), [i], [i], WriteFlags::empty()).unwrap();
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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Add some dup data
        for i in 0u8..5 {
            txn.put(db.dbi(), b"known_key", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        // iter_dup_of should not panic
        let result = cursor.iter_dup_of::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        // Consuming the iterator should not panic
        let _ = result.unwrap().count();
    }

    /// Test iter_dup_from with arbitrary key does not panic (V1).
    #[test]
    fn iter_dup_from_arbitrary_key_v1(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for i in 0u8..5 {
            txn.put(db.dbi(), b"key_a", [i], WriteFlags::empty()).unwrap();
            txn.put(db.dbi(), b"key_z", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        // iter_dup_from should not panic
        let result = cursor.iter_dup_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        // Consuming nested iterators should not panic
        for inner in result.unwrap().flatten() {
            let _ = inner.count();
        }
    }

    /// Test iter_dup_of with arbitrary key does not panic (V2).
    #[test]
    fn iter_dup_of_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for i in 0u8..5 {
            txn.put(db.dbi(), b"known_key", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_dup_of::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        let _ = result.unwrap().count();
    }

    /// Test iter_dup_from with arbitrary key does not panic (V2).
    #[test]
    fn iter_dup_from_arbitrary_key_v2(key in arb_bytes()) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for i in 0u8..5 {
            txn.put(db.dbi(), b"key_a", [i], WriteFlags::empty()).unwrap();
            txn.put(db.dbi(), b"key_z", [i], WriteFlags::empty()).unwrap();
        }

        let mut cursor = txn.cursor(db).unwrap();

        let result = cursor.iter_dup_from::<Vec<u8>, Vec<u8>>(&key);
        prop_assert!(result.is_ok());

        for inner in result.unwrap().flatten() {
            let _ = inner.count();
        }
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
        let txn = env.begin_rw_txn().unwrap();
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
        let mut txn = env.begin_rw_unsync().unwrap();
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
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Empty key should be valid
        let put_result = txn.put(db.dbi(), b"", &value, WriteFlags::empty());
        prop_assert!(put_result.is_ok());

        let get_result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            txn.get(db.dbi(), b"");
        prop_assert!(get_result.is_ok());

        let del_result = txn.del(db.dbi(), b"", None);
        prop_assert!(del_result.is_ok());
    }

    /// Test empty value handling does not panic (V1).
    #[test]
    fn empty_value_operations_v1(key in arb_small_bytes()) {
        // Skip empty keys for this test
        prop_assume!(!key.is_empty());

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Empty value should be valid
        let put_result = txn.put(db.dbi(), &key, b"", WriteFlags::empty());
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
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db.dbi(), b"", &value, WriteFlags::empty());
        prop_assert!(put_result.is_ok());

        let get_result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            txn.get(db.dbi(), b"");
        prop_assert!(get_result.is_ok());

        let del_result = txn.del(db.dbi(), b"", None);
        prop_assert!(del_result.is_ok());
    }

    /// Test empty value handling does not panic (V2).
    #[test]
    fn empty_value_operations_v2(key in arb_small_bytes()) {
        prop_assume!(!key.is_empty());

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let put_result = txn.put(db.dbi(), &key, b"", WriteFlags::empty());
        prop_assert!(put_result.is_ok());

        let get_result: signet_libmdbx::ReadResult<Option<Vec<u8>>> =
            txn.get(db.dbi(), &key);
        prop_assert!(get_result.is_ok());
        prop_assert!(get_result.unwrap().is_some());
    }

    /// Test that very large keys via txn.put() do not panic (V1).
    ///
    /// MDBX has a maximum key size that depends on page size. This test verifies
    /// that handling large keys does not cause panics via txn.put().
    ///
    /// Note: cursor.put() with oversized keys causes MDBX to abort with an
    /// assertion failure. This is why cursor_put tests use constrained key sizes.
    #[test]
    fn large_key_via_txn_put_v1(value in arb_small_bytes()) {
        // MDBX max key size with default 4KB pages is ~2022 bytes.
        // Use 4KB which may or may not exceed the limit depending on config.
        let large_key = vec![0u8; 4096];

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();

        // Large key should not panic - may succeed or return error
        let _ = txn.put(db.dbi(), &large_key, &value, WriteFlags::empty());
    }

    /// Test that very large keys via txn.put() do not panic (V2).
    #[test]
    fn large_key_via_txn_put_v2(value in arb_small_bytes()) {
        let large_key = vec![0u8; 4096];

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let mut txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let _ = txn.put(db.dbi(), &large_key, &value, WriteFlags::empty());
    }
}
