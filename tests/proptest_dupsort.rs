//! Property-based tests for DUP_SORT operations and database name handling.
//!
//! Tests focus on both "does not panic" and correctness properties. Errors are
//! acceptable (e.g., `BadValSize`), panics are not.
#![allow(missing_docs)]

use proptest::prelude::*;
use signet_libmdbx::{DatabaseFlags, Environment, WriteFlags};
use tempfile::tempdir;

/// Strategy for generating small byte vectors (0 to 64 bytes).
fn arb_small_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..64)
}

/// Strategy for valid database names (alphanumeric + underscore, 1-64 chars).
fn arb_db_name() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_]{0,63}"
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
// New: Delete specific dup
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that del with a specific value removes only that dup (V1).
    #[test]
    fn del_specific_dup_v1(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 2..8),
    ) {
        // Need at least 2 distinct non-empty values and a non-empty key
        prop_assume!(!key.is_empty());
        let mut unique: Vec<Vec<u8>> = values
            .into_iter()
            .filter(|v| !v.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        prop_assume!(unique.len() >= 2);

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Insert all unique values
        for value in &unique {
            txn.put(db, &key, value, WriteFlags::empty()).unwrap();
        }

        // Delete the first value specifically
        let to_delete = unique.remove(0);
        let deleted = txn.del(db, &key, Some(to_delete.as_slice())).unwrap();
        prop_assert!(deleted);

        // Retrieve remaining values
        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<Vec<u8>> =
            cursor.iter_dup_of::<Vec<u8>>(&key).unwrap().filter_map(Result::ok).collect();

        // The deleted value should not be present; the rest should be
        prop_assert!(!retrieved.contains(&to_delete));
        unique.sort();
        let mut retrieved_sorted = retrieved;
        retrieved_sorted.sort();
        prop_assert_eq!(retrieved_sorted, unique);
    }

    /// Test that del with a specific value removes only that dup (V2).
    #[test]
    fn del_specific_dup_v2(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 2..8),
    ) {
        prop_assume!(!key.is_empty());
        let mut unique: Vec<Vec<u8>> = values
            .into_iter()
            .filter(|v| !v.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        prop_assume!(unique.len() >= 2);

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for value in &unique {
            txn.put(db, &key, value, WriteFlags::empty()).unwrap();
        }

        let to_delete = unique.remove(0);
        let deleted = txn.del(db, &key, Some(to_delete.as_slice())).unwrap();
        prop_assert!(deleted);

        let mut cursor = txn.cursor(db).unwrap();
        let retrieved: Vec<Vec<u8>> =
            cursor.iter_dup_of::<Vec<u8>>(&key).unwrap().filter_map(Result::ok).collect();

        prop_assert!(!retrieved.contains(&to_delete));
        unique.sort();
        let mut retrieved_sorted = retrieved;
        retrieved_sorted.sort();
        prop_assert_eq!(retrieved_sorted, unique);
    }
}

// =============================================================================
// New: Delete all dups
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Test that del with None removes all dup values for the key (V1).
    #[test]
    fn del_all_dups_v1(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 1..8),
    ) {
        prop_assume!(!key.is_empty());
        let unique: Vec<Vec<u8>> = values
            .into_iter()
            .filter(|v| !v.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        prop_assume!(!unique.is_empty());

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for value in &unique {
            txn.put(db, &key, value, WriteFlags::empty()).unwrap();
        }

        // del with None deletes ALL dups for this key
        let deleted = txn.del(db, &key, None).unwrap();
        prop_assert!(deleted);

        // After deletion, get should return None
        let result: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
        prop_assert!(result.is_none());
    }

    /// Test that del with None removes all dup values for the key (V2).
    #[test]
    fn del_all_dups_v2(
        key in arb_small_bytes(),
        values in prop::collection::vec(arb_small_bytes(), 1..8),
    ) {
        prop_assume!(!key.is_empty());
        let unique: Vec<Vec<u8>> = values
            .into_iter()
            .filter(|v| !v.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        prop_assume!(!unique.is_empty());

        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for value in &unique {
            txn.put(db, &key, value, WriteFlags::empty()).unwrap();
        }

        let deleted = txn.del(db, &key, None).unwrap();
        prop_assert!(deleted);

        let result: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
        prop_assert!(result.is_none());
    }
}

// =============================================================================
// New: iter_dup completeness
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Test that iter_dup_of retrieves all inserted values for each key (V1).
    #[test]
    fn iter_dup_completeness_v1(
        n_keys in 1usize..5,
        m_values in 1usize..6,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        // Insert N keys with M values each (using deterministic byte sequences)
        for k in 0..n_keys {
            let key = vec![k as u8];
            for v in 0..m_values {
                let value = vec![v as u8];
                txn.put(db, &key, &value, WriteFlags::empty()).unwrap();
            }
        }

        // Verify each key has exactly M values via iter_dup_of
        let mut cursor = txn.cursor(db).unwrap();
        for k in 0..n_keys {
            let key = vec![k as u8];
            let retrieved: Vec<Vec<u8>> = cursor
                .iter_dup_of::<Vec<u8>>(&key)
                .unwrap()
                .filter_map(Result::ok)
                .collect();
            prop_assert_eq!(retrieved.len(), m_values);

            // Values should be in order 0..m_values
            let expected: Vec<Vec<u8>> = (0..m_values).map(|v| vec![v as u8]).collect();
            prop_assert_eq!(retrieved, expected);
        }
    }

    /// Test that iter_dup_of retrieves all inserted values for each key (V2).
    #[test]
    fn iter_dup_completeness_v2(
        n_keys in 1usize..5,
        m_values in 1usize..6,
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_unsync().unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).unwrap();

        for k in 0..n_keys {
            let key = vec![k as u8];
            for v in 0..m_values {
                let value = vec![v as u8];
                txn.put(db, &key, &value, WriteFlags::empty()).unwrap();
            }
        }

        let mut cursor = txn.cursor(db).unwrap();
        for k in 0..n_keys {
            let key = vec![k as u8];
            let retrieved: Vec<Vec<u8>> = cursor
                .iter_dup_of::<Vec<u8>>(&key)
                .unwrap()
                .filter_map(Result::ok)
                .collect();
            prop_assert_eq!(retrieved.len(), m_values);

            let expected: Vec<Vec<u8>> = (0..m_values).map(|v| vec![v as u8]).collect();
            prop_assert_eq!(retrieved, expected);
        }
    }
}
