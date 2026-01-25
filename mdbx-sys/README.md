# signet-mdbx-sys

Raw FFI bindings for [libmdbx].

## Bindings

Platform-specific bindings are pre-generated and committed:

- `src/bindings_macos.rs` - macOS and other Unix-like systems
- `src/bindings_linux.rs` - Linux
- `src/bindings_windows.rs` - Windows

### Regenerating Bindings

When updating libmdbx, regenerate bindings on each target platform.

#### Linux (via Docker)

Generate bindings:

```bash
# First build the Docker image
docker build -t mdbx-bindgen -f Dockerfile.bindgen .

# Then run bindgen inside the container
docker run --rm -v "$(pwd)":/work mdbx-bindgen \
  libmdbx/mdbx.h \
  --allowlist-var "^(MDBX|mdbx)_.*" \
  --allowlist-type "^(MDBX|mdbx)_.*" \
  --allowlist-function "^(MDBX|mdbx)_.*" \
  --no-layout-tests \
  --no-doc-comments \
  --no-prepend-enum-name \
  --merge-extern-blocks \
  -o src/bindings_linux.rs
```

#### macOS / Windows / Linux (locally)

Install bindgen-cli and run directly:

```bash
# Install bindgen-cli if not already installed
cargo install bindgen-cli

# Generate bindings for your platform
bindgen libmdbx/mdbx.h \
  --allowlist-var "^(MDBX|mdbx)_.*" \
  --allowlist-type "^(MDBX|mdbx)_.*" \
  --allowlist-function "^(MDBX|mdbx)_.*" \
  --no-layout-tests \
  --no-doc-comments \
  --no-prepend-enum-name \
  --merge-extern-blocks \
  -o src/bindings_macos.rs
  # or src/bindings_windows.rs
  # or src/bindings_linux.rs
```

Requires libclang. On macOS:

```bash
brew install llvm
export LIBCLANG_PATH=$(brew --prefix llvm)/lib
```

[libmdbx]: https://github.com/erthink/libmdbx
