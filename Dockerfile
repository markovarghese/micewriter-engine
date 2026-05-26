# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1: Build
# RocksDB compiles from C++ source by default (statically linked into binary).
# Requires a C++ toolchain + cmake.
# ---------------------------------------------------------------------------
FROM rust:1.79-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    cmake \
    pkg-config \
    libssl-dev \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependency compilation separately from source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main(){}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Build the real binary.
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---------------------------------------------------------------------------
# Stage 2: Runtime
# Only the binary + shared runtime libs (libssl, libgcc, libc).
# RocksDB is statically linked so no extra .so is needed.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 1000 -g daemon micewriter

COPY --from=builder /app/target/release/micewriter-engine /usr/local/bin/micewriter-engine

# The UDS socket and RocksDB directories are provided by k8s volumes.
# These placeholders allow local docker-run testing with bind mounts.
RUN mkdir -p /var/run/app /var/lib/rocksdb \
    && chown micewriter:daemon /var/run/app /var/lib/rocksdb

USER micewriter

ENTRYPOINT ["/usr/local/bin/micewriter-engine"]
