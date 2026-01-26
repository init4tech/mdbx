//! Utility functions for benchmarks.
#![allow(unreachable_pub)]

use signet_libmdbx::{Environment, WriteFlags};
use tempfile::{TempDir, tempdir};

/// Name of the named benchmark database.
pub const NAMED_DB: &str = "named_benchmark_db";

/// Generate a DB key string for testing.
pub fn get_key(n: u32) -> String {
    format!("key{n}")
}

// Generate a DB data string for testing.
pub fn get_data(n: u32) -> String {
    format!("data{n}")
}

/// Create a temporary benchmark database with the specified number of rows.
pub fn setup_bench_db(num_rows: u32) -> (TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().set_max_dbs(2).open(dir.path()).unwrap();

    {
        let txn = env.begin_rw_txn().unwrap();
        let db = txn.open_db(None).unwrap();
        for i in 0..num_rows {
            txn.put(db, get_key(i), get_data(i), WriteFlags::empty()).unwrap();
        }

        let named_db = txn.create_db(Some(NAMED_DB), Default::default()).unwrap();
        for i in 0..num_rows {
            txn.put(named_db, get_key(i), get_data(i), WriteFlags::empty()).unwrap();
        }
        txn.commit().unwrap();
    }
    (dir, env)
}
