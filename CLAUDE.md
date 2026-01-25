# mdbx Crate Notes

Include updates to the notes here if you discover important details while
working, or if the notes become outdated.

## Crate Overview

Rust bindings for libmdbx (MDBX database). Crate name: `signet-libmdbx`.

## Key Types

- `Environment` - Database environment (in `src/sys/environment.rs`)
- `Transaction<K>` - Transaction with kind marker RO/RW (in `src/tx/transaction.rs`)
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
    transaction.rs - Transaction impl
    iter.rs        - Iterator types
  sys/
    environment.rs - Environment impl
tests/
  cursor.rs        - Cursor tests
  transaction.rs   - Transaction tests
  environment.rs   - Environment tests
benches/
  cursor.rs        - Cursor benchmarks
```

## Testing

```bash
cargo t                          # Run all tests
cargo t --test cursor            # Run cursor tests only
cargo clippy --all-features --all-targets
cargo clippy --no-default-features --all-targets
cargo +nightly fmt
```
