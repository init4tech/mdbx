#![allow(missing_docs, dead_code)]
mod utils;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use signet_libmdbx::{Environment, WriteFlags};
use tempfile::tempdir;
use utils::quick_config;

fn setup_env() -> (tempfile::TempDir, Environment) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    (dir, env)
}

/// Benchmark: create + commit a flat (non-nested) transaction as baseline.
fn bench_flat_baseline(c: &mut Criterion) {
    let (_dir, env) = setup_env();

    c.bench_function("nested_txn::flat_baseline", |b| {
        b.iter_batched(
            || (),
            |()| {
                let txn = env.begin_rw_sync().unwrap();
                let db = txn.open_db(None).unwrap();
                txn.put(db, b"key", b"value", WriteFlags::empty()).unwrap();
                txn.commit().unwrap();
            },
            BatchSize::PerIteration,
        )
    });
}

/// Benchmark: create + commit a nested transaction at depth N.
fn bench_nested_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_txn::commit");

    for depth in [1usize, 2, 3] {
        let (_dir, env) = setup_env();

        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_batched(
                || (),
                |()| {
                    let root = env.begin_rw_sync().unwrap();
                    // Build the nesting chain. Each nested txn is committed
                    // before the parent commits.
                    let mut parents = Vec::with_capacity(depth);
                    parents.push(root);
                    for _ in 1..depth {
                        let child = parents.last().unwrap().begin_nested_txn().unwrap();
                        parents.push(child);
                    }
                    // Commit innermost to outermost.
                    for txn in parents.into_iter().rev() {
                        txn.commit().unwrap();
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

/// Benchmark: write in a nested txn, commit child, verify visible in parent.
fn bench_nested_write_and_read(c: &mut Criterion) {
    let (_dir, env) = setup_env();

    c.bench_function("nested_txn::write_in_child_read_in_parent", |b| {
        b.iter_batched(
            || (),
            |()| {
                let parent = env.begin_rw_sync().unwrap();
                let child = parent.begin_nested_txn().unwrap();

                let db = child.open_db(None).unwrap();
                child.put(db, b"nested_key", b"nested_val", WriteFlags::empty()).unwrap();
                child.commit().unwrap();

                // Value should be visible to parent after child commit.
                let db = parent.open_db(None).unwrap();
                let val: Option<Vec<u8>> = parent.get(db.dbi(), b"nested_key").unwrap();
                assert_eq!(val.as_deref(), Some(b"nested_val".as_slice()));

                parent.commit().unwrap();
            },
            BatchSize::PerIteration,
        )
    });
}

criterion_group! {
    name = benches;
    config = quick_config();
    targets = bench_flat_baseline, bench_nested_commit, bench_nested_write_and_read,
}

criterion_main!(benches);
