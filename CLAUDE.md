# mdbx Crate Notes

Include updates to the notes here if you discover important details while
working, or if the notes become outdated.

## Current TODOs:

Update these when we identify new TODOs while working on the crate. Remove them
we when complete them.

- [ ] bench adapted cursors against eachother

## Crate Overview

Rust bindings for libmdbx (MDBX database). Crate name: `signet-libmdbx`.

## MDBX Synchronization Model

When making changes to this codebase you MUST remember and conform to the MDBX
synchronization model for transactions and cursors. Access to raw pointers MUST
be mediated via the `TxAccess` trait. The table below summarizes the
transaction types and their access models.

| Transaction Type | Thread Safety | Access Model                                                                                         | enforced by Rust type system?                                      |
| ---------------- | ------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Read-Only (RO)   | !Sync + Send  | Access MUST be totally ordered and non-concurrent                                                    | No                                                                 |
| Read-Write (RW)  | !Sync + !Send | Access MUST be totally ordered, and non-concurrent. Only the creating thread may manage TX lifecycle | No                                                                 |
| Transaction<K>   | Sync + Send   | Multi-threaded wrapper using Mutex and Arc to share a RO or RW transaction safely across threads     | Yes, via synchronization wrappers                                  |
| TxUnsync<RO>     | !Sync + Send  | Single-threaded RO transaction without synchronization overhead                                      | Yes, via required &mut or & access                                 |
| TxUnsync<RW>     | !Sync + !Send | Single-threaded RW transaction without synchronization overhead                                      | Yes, &self enforces via required ownership and !Send + !Sync bound |
| Cursors          | [Inherited]   | Cursors borrow a Tx. The cursor CANNOT outlive the tx, and must reap its pointer on drop             | Yes, via lifetimes                                                 |

## Key Types

- `Environment` - Database environment (in `src/sys/environment.rs`)
- `TxSync<K>` - Transaction with kind marker RO/RW (in `src/tx/sync.rs`)
- `TxUnsync<K>` - Unsynchronized transaction with kind marker RO/RW (in `src/tx/unsync.rs`)
- `Database` - Handle to a database, stores `dbi` + `DatabaseFlags` (in `src/tx/database.rs`)
- `Cursor<'tx, K>` - Database cursor, stores `&Transaction`, raw cursor ptr, and `Database` (in `src/tx/cursor.rs`)

## API Patterns

### Cursor Creation

```rust
let db = txn.open_db(None).unwrap();  // Returns Database (has dbi + flags)
let cursor = txn.cursor(db).unwrap(); // Takes Database, NOT raw dbi
```

### Database Flags Validation

DUP_SORT/DUP_FIXED methods validate flags at runtime:

- `require_dup_sort()` returns `MdbxError::RequiresDupSort`
- `require_dup_fixed()` returns `MdbxError::RequiresDupFixed`
- `debug_assert_integer_key()` validates key length (4 or 8 bytes) in debug builds

Methods requiring DUP_SORT: `first_dup`, `last_dup`, `next_dup`, `prev_dup`, `get_both`, `get_both_range`
Methods requiring DUP_FIXED: `get_multiple`, `next_multiple`, `prev_multiple`

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
    cursor.rs      - Cursor impl
    database.rs    - Database struct
    sync.rs        - Transaction impl
    unsync.rs      - Unsynchronized transaction impl
    iter.rs        - Iterator types
  sys/
    environment.rs - Environment impl
tests/
  cursor.rs        - Cursor tests
  transaction.rs   - Transaction tests
  environment.rs   - Environment tests
benches/
  cursor.rs        - Cursor benchmarks
  transaction.rs   - Transaction benchmarks
  db_open.rs       - Database open benchmarks
  utils.rs         - Benchmark utilities
```

## Testing

```bash
cargo t                          # Run all tests
cargo t --test cursor            # Run cursor tests only
cargo clippy --all-features --all-targets
cargo clippy --no-default-features --all-targets
cargo +nightly fmt
```
