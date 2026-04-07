#![allow(missing_docs, dead_code)]
mod utils;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, prelude::SliceRandom, rngs::StdRng};
use signet_libmdbx::{Environment, ObjectLength};
use std::borrow::Cow;
use tempfile::tempdir;
use utils::{
    bench_key, bench_value_sized, is_bench_full, quick_config, setup_bench_env,
    setup_bench_env_sized,
};

const COLD_N_ROWS: u32 = 1_000_000;
const COLD_LOOKUPS: u32 = 1_000;

const ENTRY_COUNTS_FULL: &[u32] = &[100, 1_000, 10_000, 100_000];
const ENTRY_COUNTS_QUICK: &[u32] = &[100, 1_000, 10_000];
/// Value sizes for benchmarks.
const VALUE_SIZES: &[usize] = &[32, 128, 512];

fn entry_counts() -> &'static [u32] {
    use std::sync::Once;
    static WARN: Once = Once::new();
    if is_bench_full() {
        ENTRY_COUNTS_FULL
    } else {
        WARN.call_once(|| {
            eprintln!("NOTE: skipping 100K entry benchmarks (set BENCH_FULL=1 for full suite)");
        });
        ENTRY_COUNTS_QUICK
    }
}

fn bench_sequential_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::sequential_get");

    for &size in VALUE_SIZES {
        for &n in entry_counts() {
            let (_dir, env) = setup_bench_env_sized(n, size);
            let keys: Vec<[u8; 32]> = (0..n).map(bench_key).collect();
            // Open the db handle once — dbi is stable for the environment lifetime.
            let db = {
                let txn = env.begin_ro_unsync().unwrap();
                txn.open_db(None).unwrap()
            };

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter(|| {
                    let txn = env.begin_ro_unsync().unwrap();
                    let mut total = 0usize;
                    for key in &keys {
                        let val: Cow<'_, [u8]> =
                            txn.get(db.dbi(), key.as_slice()).unwrap().unwrap();
                        total += val.len();
                    }
                    total
                })
            });
        }
    }
    group.finish();
}

fn bench_random_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::random_get");

    for &size in VALUE_SIZES {
        for &n in entry_counts() {
            let (_dir, env) = setup_bench_env_sized(n, size);
            let mut keys: Vec<[u8; 32]> = (0..n).map(bench_key).collect();
            keys.shuffle(&mut StdRng::from_seed(Default::default()));
            // Open the db handle once — dbi is stable for the environment lifetime.
            let db = {
                let txn = env.begin_ro_unsync().unwrap();
                txn.open_db(None).unwrap()
            };

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter(|| {
                    let txn = env.begin_ro_unsync().unwrap();
                    let mut total = 0usize;
                    for key in &keys {
                        let val: Cow<'_, [u8]> =
                            txn.get(db.dbi(), key.as_slice()).unwrap().unwrap();
                        total += val.len();
                    }
                    total
                })
            });
        }
    }
    group.finish();
}

fn bench_full_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::full_iteration");

    for &size in VALUE_SIZES {
        for &n in entry_counts() {
            let (_dir, env) = setup_bench_env_sized(n, size);
            // Open the db handle once — dbi is stable for the environment lifetime.
            let db = {
                let txn = env.begin_ro_unsync().unwrap();
                txn.open_db(None).unwrap()
            };

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter(|| {
                    let txn = env.begin_ro_unsync().unwrap();
                    let mut cursor = txn.cursor(db).unwrap();
                    let mut count = 0usize;
                    while cursor.next::<ObjectLength, ObjectLength>().unwrap().is_some() {
                        count += 1;
                    }
                    count
                })
            });
        }
    }
    group.finish();
}

fn bench_append_ordered_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling::append_ordered_put");

    for &size in VALUE_SIZES {
        for &n in entry_counts() {
            let items: Vec<([u8; 32], Vec<u8>)> =
                (0..n).map(|i| (bench_key(i), bench_value_sized(i, size))).collect();

            group.bench_with_input(BenchmarkId::new(format!("{size}B"), n), &n, |b, _| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let env = Environment::builder().open(dir.path()).unwrap();
                        (dir, env)
                    },
                    |(_dir, env)| {
                        let txn = env.begin_rw_unsync().unwrap();
                        let db = txn.open_db(None).unwrap();
                        for (key, data) in &items {
                            txn.append(db, key.as_slice(), data.as_slice()).unwrap();
                        }
                        txn.commit().unwrap();
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
    group.finish();
}

/// Evicts OS page cache for the mdbx data file. Must be called while no
/// mmap exists on the file (i.e. after closing the environment) so the
/// kernel is free to drop the pages. `posix_fadvise` is advisory but
/// reliable when no active mappings pin the pages.
#[cfg(target_os = "linux")]
fn evict_os_cache(dir: &std::path::Path) {
    use std::os::unix::io::AsRawFd;
    let data_path = dir.join("mdbx.dat");
    let file = std::fs::File::open(&data_path).unwrap();
    // SAFETY: fd is valid from File::open.
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    assert_eq!(rc, 0, "posix_fadvise failed: {rc}");
    // File (and fd) dropped here.
}

#[cfg(not(target_os = "linux"))]
fn evict_os_cache(_dir: &std::path::Path) {
    // posix_fadvise not available on macOS; reads will be warm.
}

fn bench_cold_random_get(c: &mut Criterion) {
    let (dir, env) = setup_bench_env(COLD_N_ROWS);
    // Drop the env so the mmap is unmapped before we evict cache.
    drop(env);

    let mut rng = StdRng::seed_from_u64(42);
    let indices: Vec<u32> = (0..COLD_LOOKUPS).map(|_| rng.random_range(0..COLD_N_ROWS)).collect();

    c.bench_function("cold_random_get", |b| {
        b.iter(|| {
            evict_os_cache(dir.path());
            let env = Environment::builder().open(dir.path()).unwrap();
            let db = {
                let txn = env.begin_ro_unsync().unwrap();
                txn.open_db(None).unwrap()
            };
            for &i in &indices {
                let key = bench_key(i);
                let txn = env.begin_ro_unsync().unwrap();
                let val: Option<Cow<'_, [u8]>> = txn.get(db.dbi(), key.as_slice()).unwrap();
                assert!(val.is_some());
            }
        });
    });
}

fn bench_cold_sequential_scan(c: &mut Criterion) {
    let (dir, env) = setup_bench_env(COLD_N_ROWS);
    drop(env);

    c.bench_function("cold_sequential_scan", |b| {
        b.iter(|| {
            evict_os_cache(dir.path());
            let env = Environment::builder().open(dir.path()).unwrap();
            let db = {
                let txn = env.begin_ro_unsync().unwrap();
                txn.open_db(None).unwrap()
            };
            let txn = env.begin_ro_unsync().unwrap();
            let mut cursor = txn.cursor(db).unwrap();
            let mut count = 0u32;
            while cursor.next::<ObjectLength, ObjectLength>().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, COLD_N_ROWS);
        });
    });
}

criterion_group! {
    name = benches;
    config = quick_config();
    targets =
        bench_sequential_get,
        bench_random_get,
        bench_full_iteration,
        bench_append_ordered_put,
        bench_cold_random_get,
        bench_cold_sequential_scan,
}

criterion_main!(benches);
