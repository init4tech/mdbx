# mdbx Crate Notes

Include updates to the notes here if you discover important details while
working, or if the notes become outdated.

## Crate Overview

Rust bindings for libmdbx (MDBX database). Crate name: `signet-libmdbx`.

## Crate Mandates

- You MUST NOT expose raw pointers to MDBX types via getters.
- You MUST maintain zero-copy semantics for read operations in all new
  interfaces.
- You MUST read and respect `SAFETY` comments throughout the codebase.
- You MUST NOT introduce new dependencies without approval.
- All FFI calls MUST be made in the `ops` module.
- All access of the ops module MUST be done within a `with_txn_ptr` closure.

## MDBX Synchronization Model

When making changes to this codebase you MUST remember and conform to the MDBX
synchronization model for transactions and cursors. Access to raw pointers MUST
be mediated via the `TxAccess` trait.

## Key Types

- `Environment` - Database environment (in `src/sys/environment.rs`)
- `TxSync<K>` - Transaction with kind marker RO/RW (in `src/tx/sync.rs`)
- `TxUnsync<K>` - Unsynchronized transaction with kind marker RO/RW (in `src/tx/unsync.rs`)
- `Database` - Handle to a database, stores `dbi` + `DatabaseFlags` (in `src/tx/database.rs`)
- `Cursor<'tx, K>` - Database cursor, stores `&Transaction`, raw cursor ptr, and `Database` (in `src/tx/cursor.rs`)

## API Patterns

### Cursor Creation and Caching

```rust
let db = txn.open_db(None).unwrap();  // Returns Database (has dbi + flags)
let cursor = txn.cursor(db).unwrap(); // Takes Database, NOT raw dbi
```

Cursors are transparently cached within transactions. When a cursor is
dropped, its raw pointer is returned to the transaction's cache. Subsequent
`cursor()` calls reuse cached pointers, avoiding `mdbx_cursor_open`/
`mdbx_cursor_close` overhead (~100 ns per cycle). The cache is drained
and all pointers closed on commit or abort.

`DbCache` (in `src/tx/cache.rs`) stores raw `*mut ffi::MDBX_cursor`
pointers, which makes it `!Send + !Sync` by default. Explicit `unsafe impl
Send + Sync for DbCache` is required because:
- `SyncKind::Cache` requires `Cache + Send` (for `RefCell<DbCache>: Send`)
- `SharedCache` uses `Arc<RwLock<DbCache>>` which requires `DbCache: Send + Sync`
- This is sound because `Cursor` itself is already `unsafe impl Send + Sync`,
  and all access to cached pointers is mediated by `RefCell` (unsync) or
  `RwLock` (sync)

### Database Flags Validation

DUP_SORT/DUP_FIXED methods validate flags at runtime:

- `require_dup_sort()` returns `MdbxError::RequiresDupSort`
- `require_dup_fixed()` returns `MdbxError::RequiresDupFixed`

Methods requiring DUP_SORT: `first_dup`, `last_dup`, `next_dup`, `prev_dup`, `get_both`, `get_both_range`
Methods requiring DUP_FIXED: `get_multiple`, `next_multiple`, `prev_multiple`

### Input Validation Model

MDBX's C layer aborts the process (via `cASSERT`) on certain constraint
violations — notably INTEGER_KEY size mismatches and oversized keys/values.
These aborts cannot be caught.

Our validation model (in `src/tx/assertions.rs`):

- **Debug builds:** `debug_assert` checks catch constraint violations in
  Rust before they reach FFI. This includes key/value size limits, INTEGER_KEY
  length (must be 4 or 8 bytes), INTEGER_DUP length, and append ordering.
- **Release builds:** No checks are performed. Invalid input passes through
  to MDBX, which may abort the process.
- **Benchmarks and fuzz targets:** MUST constrain inputs to valid ranges.
  Do not feed arbitrary-length keys to INTEGER_KEY databases or oversized
  keys/values to any database. The fuzz/bench harness is responsible for
  generating valid input, not the library.

This is intentional. The library trusts callers in release mode for
performance. The debug assertions exist to catch bugs during development.

### Error Types

- `MdbxError` - FFI/database errors (in `src/error.rs`)
- `ReadError` - Wraps MdbxError + decoding errors for read operations
- `MdbxResult<T>` = `Result<T, MdbxError>`
- `ReadResult<T>` = `Result<T, ReadError>`

## File Layout

```
src/
  lib.rs           - Re-exports
  error.rs         - MdbxError, ReadError
  flags.rs         - DatabaseFlags, WriteFlags, etc.
  codec.rs         - TableObject trait
  tx/
    mod.rs
    assertions.rs  - Debug assertions for key/value constraints
    cursor.rs      - Cursor impl
    database.rs    - Database struct
    sync.rs        - Transaction impl
    unsync.rs      - Unsynchronized transaction impl
    iter.rs        - Iterator types
  sys/
    environment.rs - Environment impl
tests/
  cursor.rs            - Cursor tests
  transaction.rs       - Transaction tests
  environment.rs       - Environment tests
  proptest_kv.rs       - Property tests: key/value operations
  proptest_cursor.rs   - Property tests: cursor operations
  proptest_dupsort.rs  - Property tests: DUPSORT operations
  proptest_dupfixed.rs - Property tests: DUPFIXED operations
  proptest_iter.rs     - Property tests: iterator operations
  proptest_nested.rs   - Property tests: nested transactions
benches/
  cursor.rs        - Cursor read benchmarks
  cursor_write.rs  - Cursor write benchmarks
  transaction.rs   - Transaction benchmarks
  db_open.rs       - Database open benchmarks
  reserve.rs       - Reserve vs put benchmarks
  nested_txn.rs    - Nested transaction benchmarks
  concurrent.rs    - Concurrency benchmarks
  scaling.rs       - Scaling benchmarks
  deletion.rs      - Deletion benchmarks
  iter.rs          - Iterator benchmarks
  utils.rs         - Benchmark utilities
fuzz/
  fuzz_targets/    - cargo-fuzz targets (FFI/unsafe boundary hardening)
```

## Testing

```bash
cargo t                          # Run all tests
cargo t --test cursor            # Run cursor tests only
cargo clippy --all-features --all-targets
cargo clippy --no-default-features --all-targets
cargo +nightly fmt
```

## Linux Testing

Before committing, run the Docker Linux environment to verify changes pass on Linux:

```bash
docker build -t mdbx-linux-tests . && docker run --rm mdbx-linux-tests
```

This SHOULD be run alongside local tests and linting, especially for changes that:

- Modify build configuration
- Add new dependencies
- Change platform-specific code

