FROM ubuntu:24.04

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Rust (latest stable)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install nightly for rustfmt
RUN rustup toolchain install nightly --component rustfmt

WORKDIR /work

# Copy source
COPY . .

# Default: run full checks (fmt, clippy, tests)
CMD ["sh", "-c", "cargo +nightly fmt --check && \
    cargo clippy --all-features --all-targets -- -D warnings && \
    cargo clippy --no-default-features --all-targets -- -D warnings && \
    cargo test --all-features && \
    cargo test --no-default-features"]
