//! Property-based tests for nested transaction behavior (TxSync / V1 only).
//!
//! Tests focus on correctness of commit/abort semantics. Errors are
//! acceptable (e.g., `BadValSize`), panics are not.
#![allow(missing_docs)]

use proptest::prelude::*;
use signet_libmdbx::{Environment, WriteFlags};
use tempfile::tempdir;

/// Strategy for keys that won't trigger MDBX assertion failures (non-empty).
fn arb_safe_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..512)
}

// =============================================================================
// Nested commit preserves writes
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Test that a write in a committed child transaction is visible in the parent (V1).
    #[test]
    fn nested_commit_preserves_writes(
        key in arb_safe_key(),
        value in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        {
            let nested = txn.begin_nested_txn().unwrap();
            let nested_db = nested.open_db(None).unwrap();
            let put_result = nested.put(nested_db, &key, &value, WriteFlags::empty());
            if put_result.is_ok() {
                nested.commit().unwrap();

                // After commit, parent should see the value.
                let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
                prop_assert_eq!(retrieved, Some(value));
            }
        }
    }
}

// =============================================================================
// Nested abort discards writes
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Test that a write in a dropped (aborted) child transaction is NOT visible in the parent (V1).
    #[test]
    fn nested_abort_discards_writes(
        key in arb_safe_key(),
        value in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Confirm the key is not yet present.
        let before: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
        prop_assume!(before.is_none());

        {
            let nested = txn.begin_nested_txn().unwrap();
            let nested_db = nested.open_db(None).unwrap();
            let put_result = nested.put(nested_db, &key, &value, WriteFlags::empty());
            if put_result.is_ok() {
                // Drop without committing — this aborts the nested transaction.
                drop(nested);

                // Parent should NOT see the value.
                let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &key).unwrap();
                prop_assert!(retrieved.is_none());
            }
        }
    }
}

// =============================================================================
// Parent writes survive child abort
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Test that parent writes survive a child abort, and that the child's write is discarded (V1).
    #[test]
    fn parent_writes_survive_child_abort(
        parent_key in arb_safe_key(),
        parent_value in prop::collection::vec(any::<u8>(), 1..64),
        child_key in arb_safe_key(),
        child_value in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        let dir = tempdir().unwrap();
        let env = Environment::builder().open(dir.path()).unwrap();
        let txn = env.begin_rw_sync().unwrap();
        let db = txn.open_db(None).unwrap();

        // Write in parent.
        let put_result = txn.put(db, &parent_key, &parent_value, WriteFlags::empty());
        if put_result.is_err() {
            return Ok(());
        }

        {
            let nested = txn.begin_nested_txn().unwrap();
            let nested_db = nested.open_db(None).unwrap();
            // Write something in the child (ignore errors).
            let _ = nested.put(nested_db, &child_key, &child_value, WriteFlags::empty());
            // Abort by dropping.
            drop(nested);
        }

        // Parent write must still be visible.
        let retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &parent_key).unwrap();
        prop_assert_eq!(retrieved, Some(parent_value));

        // If the keys differ, child write must NOT be visible.
        if parent_key != child_key {
            let child_retrieved: Option<Vec<u8>> = txn.get(db.dbi(), &child_key).unwrap();
            prop_assert!(child_retrieved.is_none());
        }
    }
}
