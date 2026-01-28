# Issue: Cursor and Iterator Benchmark Regression

the baseline `cursor::traverse::raw` is 606.19 ns.

this branch causes `cursor::traverse::iter::single_thread` performance to
degrade from 784.02 ns to 995.32 ns

and `cursor:traverse::iter` perf to degrade from 847.02 ns to 1,015.2 tns

Given that both of these benchmarks have similar degradation, the root cause
is likely the same.

## Summary

Cursor and iterator benchmarks have regressed. The root cause is mutex lock
acquisition on every cursor operation for synchronized transactions.

## Root Cause

### Hot Path Analysis

Every iterator step calls `execute_op` (iter.rs:215-238):

```rust
fn execute_op(&self) -> ReadResult<KvOpt<'tx, A, Key, Value>> {
    let access = self.cursor.access();
    access.with_txn_ptr(|txn| {          // ← Called for EVERY item
        ffi::mdbx_cursor_get(...)
    })?
}
```

For `PtrSyncInner` (synchronized transactions), `with_txn_ptr` acquires a
mutex (access.rs:643-653):

```rust
fn with_txn_ptr<F, R>(&self, f: F) -> MdbxResult<R> {
    let timeout_flag = self.lock();      // ← MUTEX LOCK per item
    if *timeout_flag {
        return Err(MdbxError::ReadTransactionTimeout);
    }
    let result = f(self.txn);
    Ok(result)
}
```

### Impact

With 100 items in the benchmark:

- **100+ mutex lock/unlock cycles** in a tight loop
- Each lock involves atomic operations and potential thread contention
- Completely dominates the actual FFI call time

### Why single_thread Benchmarks Are Faster

`TxUnsync` uses `RoGuard` which has no mutex (access.rs:396-415):

```rust
// RoGuard::with_txn_ptr - no mutex, just Arc operations
fn with_txn_ptr<F, R>(&self, f: F) -> MdbxResult<R> {
    if let Some(strong) = self.try_ref() {  // Arc upgrade (atomic, no mutex)
        return Ok(f(strong.ptr));
    }
    Err(MdbxError::ReadTransactionTimeout)
}
```

Arc atomic operations are much cheaper than mutex lock/unlock.

### Comparison with Raw FFI

The raw benchmark (cursor.rs:219-238) shows baseline performance:

```rust
while mdbx_cursor_get(cursor, &mut key, &mut data, MDBX_NEXT) == 0 {
    // No locking, no checks, just FFI
}
```

## Benchmark Structure

```
benches/cursor.rs:
  cursor::traverse::iter          - sync tx, uses PtrSyncInner (slow)
  cursor::traverse::iter_x3       - sync tx, 3 iterations
  cursor::traverse::for_loop      - sync tx, explicit loop
  cursor::traverse::raw           - raw FFI baseline (fast)
  cursor::traverse::iter::single_thread     - unsync tx, RoGuard (faster)
  cursor::traverse::iter_x3::single_thread  - unsync tx, 3 iterations
  cursor::traverse::for_loop::single_thread - unsync tx, explicit loop
```

## Potential Fixes

### Option 1: Guarded Iteration

Hold lock for entire iteration session:

```rust
impl Iter {
    fn with_guard<F, R>(&mut self, f: F) -> MdbxResult<R>
    where
        F: FnOnce(&mut Self) -> R,
    {
        let _guard = self.cursor.access().try_guard()?;
        Ok(f(self))
    }
}

// Usage: hold lock, iterate all items
cursor.iter().with_guard(|iter| {
    for item in iter { ... }
})
```

### Option 2: Cached Validity Check

Cache timeout check result per iteration batch:

```rust
struct Iter {
    // ... existing fields
    validity_token: Option<ValidityToken>,
}

impl Iter {
    fn execute_op(&mut self) -> ReadResult<...> {
        // Revalidate periodically, not every call
        if self.validity_token.is_none() || self.validity_token.expired() {
            self.validity_token = Some(self.cursor.access().validate()?);
        }
        // Fast path: use cached pointer
        unsafe { ffi::mdbx_cursor_get(...) }
    }
}
```

### Option 3: Lock-Free Fast Path for RO

For read-only transactions that haven't timed out, skip locking:

```rust
fn with_txn_ptr<F, R>(&self, f: F) -> MdbxResult<R> {
    // Fast path: check timeout flag without lock (relaxed read)
    if !self.timeout_flag.load(Ordering::Relaxed) {
        return Ok(f(self.txn));
    }
    // Slow path: acquire lock, recheck
    let timeout_flag = self.lock();
    if *timeout_flag {
        return Err(MdbxError::ReadTransactionTimeout);
    }
    Ok(f(self.txn))
}
```

**Warning**: This has subtle correctness implications. The monitor sets the
flag while holding the lock, so a relaxed read could see stale data. Would
need careful analysis.

### Option 4: Batch Operations

Add batch cursor operations that acquire lock once:

```rust
impl Cursor {
    fn collect_n<Key, Value>(&mut self, n: usize) -> MdbxResult<Vec<(Key, Value)>> {
        self.access().with_txn_ptr(|txn| {
            let mut results = Vec::with_capacity(n);
            for _ in 0..n {
                // All FFI calls under single lock
                match unsafe { ffi::mdbx_cursor_get(...) } {
                    0 => results.push(decode(...)),
                    MDBX_NOTFOUND => break,
                    err => return Err(err),
                }
            }
            Ok(results)
        })
    }
}
```

## Key Files

- `src/tx/iter.rs:215-238` - execute_op, the hot path
- `src/tx/access.rs:643-653` - PtrSyncInner::with_txn_ptr (mutex)
- `src/tx/access.rs:396-415` - RoGuard::with_txn_ptr (no mutex)
- `benches/cursor.rs` - benchmark definitions

## Recommendation

Option 1 (guarded iteration) is likely the cleanest solution:

- Maintains correctness guarantees
- Single lock acquisition per iteration session
- Compatible with existing API (can be opt-in)
- Clear semantics: "I'm iterating, don't timeout during this"

The guard would need to prevent timeout while held, similar to how
`SyncTxGuard` already works.
