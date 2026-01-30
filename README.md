# signet-libmdbx

Rust bindings for [libmdbx].

Forked from [reth-libmdbx], which was
forked from an earlier Apache licenced version of the `libmdbx-rs` crate.

NOTE: Most of the repo came from [lmdb-rs bindings].

## Differences from reth-libmdbx

- Improve documentation :)
- Add [`TxUnsync`] type for single-threaded transactions.
  - These may be up to 3x faster than the thread-safe versions.
- Rename [`Transaction`] to [`TxSync`] for clarity.
- Improve support for custom `TableObject` types.
  - Added `TableObjectOwned` trait to represent types that can be deserialized
    from a database table without borrowing.
  - Added `ReadError` error type to represent errors that can occur when
    reading from the database. This captures MDBX errors as well as codec
    specific errors.
- More-accurate lifetime semantics
  - Cursors now have lifetimes tied to the transaction they were created from.
  - Cursors CANNOT hold transactions open.
  - All DB reads borrow from the transaction when available.
- API consistency review
  - `iter` and `iter_dup` now have consistent behavior (previously, `iter`
    would start at the next key, while `iter_dup` would start at the current
    key).
  - Iteration methods that reposition the cursor now do so BEFORE returning the
    iterator.
- Module layout changes
  - `sys` - Environment and transaction management.
  - `tx` - module contains transactions, cursors, and iterators

## Updating the libmdbx Version

To update the libmdbx version you must clone it and copy the `dist/` folder in
`mdbx-sys/`.
Make sure to follow the [building steps].

```bash
# clone libmdbx to a repository outside at specific tag
git clone https://github.com/erthink/libmdbx.git ../libmdbx --branch v0.7.0
make -C ../libmdbx dist

# copy the `libmdbx/dist/` folder just created into `mdbx-sys/libmdbx`
rm -rf mdbx-sys/libmdbx
cp -R ../libmdbx/dist mdbx-sys/libmdbx

# add the changes to the next commit you will make
git add mdbx-sys/libmdbx
```

## Linux Testing

Run tests in a Linux environment (Ubuntu 24.04):

```bash
# Build the test image
docker build -t mdbx-linux-tests .

# Run full checks (fmt, clippy, tests)
docker run --rm mdbx-linux-tests

# Run specific commands
docker run --rm mdbx-linux-tests cargo test --all-features
docker run --rm mdbx-linux-tests cargo clippy --all-features --all-targets
```

[libmdbx]: https://github.com/erthink/libmdbx
[reth-libmdbx]: https://github.com/paradigmxyz/reth
[building steps]: https://github.com/erthink/libmdbx#building
[lmdb-rs bindings]: https://github.com/mozilla/lmdb-rs
