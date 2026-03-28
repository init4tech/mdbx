//! Property-based tests for cursor operations.
//!
//! Tests focus on both "does not panic" and correctness properties. Errors are
//! acceptable (e.g., `BadValSize`), panics are not.
#![allow(missing_docs)]

use proptest::prelude::*;
use signet_libmdbx::{Environment, WriteFlags};
use tempfile::tempdir;

/// Strategy for generating byte vectors of various sizes (0 to 1KB).
fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..1024)
}

/// Strategy for keys that won't trigger MDBX assertion failures.
/// MDBX max key size is ~2022 bytes for 4KB pages.
fn arb_safe_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..512)
}

// =============================================================================
// Cursor Operations - TxSync (V1)
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
}

// =============================================================================
// Cursor Operations - TxUnsync (V2)
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

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
// Cursor Put Operations
// =============================================================================

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
// New: Cursor set_lowerbound
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that set_lowerbound returns a key >= the search key when Some (V1).
    #[test]
    fn cursor_set_lowerbound_v1(
        entries in prop::collection::vec((arb_safe_key(), arb_bytes()), 1..10),
        search_key in arb_safe_key(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        for (key, value) in &entries {
            // Ignore errors (e.g. empty key issues)
            let _ = txn.put(db, key, value, WriteFlags::empty());
        }

        let mut cursor = txn.cursor(db).unwrap();
        let result = cursor.set_lowerbound::<Vec<u8>, Vec<u8>>(&search_key);
        prop_assert!(result.is_ok());

        if let Some((_exact, returned_key, _val)) = result.unwrap() {
            // The returned key must be >= the search key
            prop_assert!(returned_key >= search_key);
        }
    }

    /// Test that set_lowerbound returns a key >= the search key when Some (V2).
    #[test]
    fn cursor_set_lowerbound_v2(
        entries in prop::collection::vec((arb_safe_key(), arb_bytes()), 1..10),
        search_key in arb_safe_key(),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        for (key, value) in &entries {
            let _ = txn.put(db, key, value, WriteFlags::empty());
        }

        let mut cursor = txn.cursor(db).unwrap();
        let result = cursor.set_lowerbound::<Vec<u8>, Vec<u8>>(&search_key);
        prop_assert!(result.is_ok());

        if let Some((_exact, returned_key, _val)) = result.unwrap() {
            prop_assert!(returned_key >= search_key);
        }
    }
}

// =============================================================================
// New: Cursor append sorted
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that appending sorted keys via cursor then iterating retrieves all in order (V1).
    #[test]
    fn cursor_append_sorted_v1(
        raw_keys in prop::collection::vec(arb_safe_key(), 1..20),
    ) {
        // Filter out empty keys (MDBX allows empty keys but let's keep it simple)
        let mut keys: Vec<Vec<u8>> = raw_keys.into_iter().filter(|k| !k.is_empty()).collect();
        prop_assume!(!keys.is_empty());

        keys.sort();
        keys.dedup();

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        // Append all keys in sorted order
        for key in &keys {
            cursor.append(key, b"v").unwrap();
        }

        // Iterate and verify all keys are present in order
        let mut read_cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<Vec<u8>> = read_cursor
            .iter_start::<Vec<u8>, Vec<u8>>()
            .unwrap()
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();

        prop_assert_eq!(retrieved, keys);
    }

    /// Test that appending sorted keys via cursor then iterating retrieves all in order (V2).
    #[test]
    fn cursor_append_sorted_v2(
        raw_keys in prop::collection::vec(arb_safe_key(), 1..20),
    ) {
        let mut keys: Vec<Vec<u8>> = raw_keys.into_iter().filter(|k| !k.is_empty()).collect();
        prop_assume!(!keys.is_empty());

        keys.sort();
        keys.dedup();

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.open_db(None).unwrap();

        let mut cursor = txn.cursor(db).unwrap();

        for key in &keys {
            cursor.append(key, b"v").unwrap();
        }

        let mut read_cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<Vec<u8>> = read_cursor
            .iter_start::<Vec<u8>, Vec<u8>>()
            .unwrap()
            .filter_map(Result::ok)
            .map(|(k, _)| k)
            .collect();

        prop_assert_eq!(retrieved, keys);
    }
}
