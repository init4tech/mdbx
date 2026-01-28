# Issue: Readers Not Being Cleaned Up

## Summary

Read-only transactions (readers) are not being properly cleaned up. This
investigation documents the reader lifecycle and potential causes.

## Architecture

### Type Hierarchy

```
TxSync<K>
  └── inner: Arc<SyncInner<K>>
        ├── ptr: PtrSync<K>
        │     └── inner: Arc<PtrSyncInner<K>>  ← actual txn pointer lives here
        └── db_cache: SharedCache
```

### Reader Tracking (read-tx-timeouts feature)

When `read-tx-timeouts` is enabled, the `TxnManager` maintains:

- `active: DashMap<usize, (PtrSync<RO>, Instant, Option<Backtrace>)>` - currently
  active RO transactions
- `timed_out_not_aborted: DashSet<usize>` - transactions that timed out but
  user hasn't dropped yet

## Lifecycle

### Creation (sync.rs:567-585)

```rust
impl TxSync<RO> {
    pub(crate) fn new(env: Environment) -> MdbxResult<Self> {
        // 1. Create raw MDBX transaction via FFI
        mdbx_txn_begin_ex(..., &mut txn, ...);

        // 2. Wrap in TxSync
        let this = Self::new_from_ptr(env, txn);

        // 3. Clone PtrSync and store in tracking map
        #[cfg(feature = "read-tx-timeouts")]
        this.env().txn_manager().add_active_read_transaction(txn, this.inner.ptr.clone());
        //                                                        ^^^^^^^^^^^^^^^^^^^^
        //                                     Arc<PtrSyncInner> refcount becomes 2

        Ok(this)
    }
}
```

### Expected Cleanup Path

1. User drops `TxSync`
2. `Arc<SyncInner>` refcount → 0
3. `SyncInner::drop` runs (sync.rs:325-340):
   - Calls `remove_active_read_transaction(ptr)` → removes from map
   - Map's `PtrSync` clone is dropped → Arc refcount decreases
4. `SyncInner.ptr` (PtrSync) is dropped → Arc refcount → 0
5. `PtrSyncInner::drop` runs (access.rs:667-692):
   - Calls `remove_active_read_transaction` again (no-op, already removed)
   - Calls `mdbx_txn_abort(ptr)` → **reader slot released**

### Timeout Path (txn_manager.rs:231-318)

The timeout monitor thread:

1. Iterates `active` transactions
2. For transactions exceeding `max_duration`:
   - Acquires lock on `PtrSyncInner`
   - Calls `mdbx_txn_reset(ptr)` - **parks reader, does NOT release slot**
   - Sets timeout flag to `true`
   - Removes from `active` map
   - Adds to `timed_out_not_aborted` set

**Critical**: `mdbx_txn_reset` parks the reader but the reader slot remains
occupied. Only `mdbx_txn_abort` releases it.

## Potential Issues

### 1. Timed-out transactions with live handles

If a transaction times out but the user still holds `TxSync`:

- Monitor calls `mdbx_txn_reset` (reader parked, slot occupied)
- Monitor removes from `active`, adds to `timed_out_not_aborted`
- User still holds `TxSync` → `PtrSyncInner` still alive
- Reader slot stays occupied until user drops `TxSync`

If users leak `TxSync` handles (never drop them), reader slots accumulate.

### 2. Arc reference in map prevents cleanup

The map stores `PtrSync<RO>` which holds `Arc<PtrSyncInner<RO>>`. If
`SyncInner::drop` fails to remove from the map for any reason, the Arc
reference keeps `PtrSyncInner` alive, preventing `mdbx_txn_abort`.

### 3. Double-removal timing

Both `SyncInner::drop` and `PtrSyncInner::drop` call
`remove_active_read_transaction`. This is intentional (belt and suspenders)
but relies on the map operations being idempotent.

## Key Files

- `src/tx/sync.rs:567-585` - TxSync<RO>::new, adds to tracking
- `src/tx/sync.rs:325-340` - SyncInner::drop, removes from tracking
- `src/tx/access.rs:667-692` - PtrSyncInner::drop, calls mdbx_txn_abort
- `src/sys/txn_manager.rs:156-176` - add/remove tracking methods
- `src/sys/txn_manager.rs:231-318` - timeout monitor thread

## Investigation Steps

1. Add tracing to `add_active` and `remove_active` to verify calls match up
2. Check `Arc::strong_count` on `PtrSyncInner` at key points
3. Monitor `timed_out_not_aborted.len()` over time
4. Check if `TxSync` handles are being leaked (never dropped)
5. Verify `mdbx_txn_abort` is actually being called in `PtrSyncInner::drop`

## Questions to Answer

- Are `TxSync` handles being leaked somewhere?
- Is `SyncInner::drop` running for all transactions?
- Is `remove_active_read_transaction` succeeding?
- What's the state of `timed_out_not_aborted` over time?
