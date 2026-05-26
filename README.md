# micewriter-engine
> Part of the [mIceWriter Ingestion Ecosystem](../micewriter-hub/README.md)

Memory-safe Rust sidecar engine. Accepts telemetry records over a Unix Domain Socket, buffers them in a local RocksDB instance, and flushes them as Parquet files to an Apache Iceberg table (via Nessie REST Catalog + MinIO S3) on a jittered 10-minute cycle.

## Architecture

```
Java SDK  ──UDS──►  uds_server.rs  ──►  rocksdb_store.rs (active CF)
                                                │
                                      (every ~10 min)
                                                │
                               flush_engine.rs  ▼
                                  rotate CF ──► parquet_writer.rs ──► iceberg_writer.rs
                                                                          │
                                                              MinIO S3 + Nessie commit
```

## Source Layout

| File | Responsibility |
|------|---------------|
| `main.rs` | Entry point, SIGTERM handler, emergency flush |
| `config.rs` | Env-var configuration |
| `protocol.rs` | IPC message types (`RegisterSchema`, `IngestRecord`, `AckResponse`) |
| `uds_server.rs` | Async Tokio UDS listener + frame parser |
| `rocksdb_store.rs` | Active/frozen CF rotation and record append |
| `flush_engine.rs` | Jittered cron loop, orchestrates compile → upload → commit |
| `parquet_writer.rs` | `IngestRecord[]` → Arrow `RecordBatch` → Parquet bytes |
| `iceberg_writer.rs` | Iceberg REST catalog ops (create table, `fast_append`, commit) |

## IPC Protocol

All frames use a **4-byte big-endian length prefix** followed by:

| Byte 0 | Remaining bytes | Direction |
|--------|----------------|-----------|
| `0x01` | JSON `RegisterSchema` | SDK → Engine |
| `0x02` | Native Arrow IPC `IngestRecord` | SDK → Engine |
| *(any)* | JSON `AckResponse` | Engine → SDK |

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CATALOG_TYPE` | no | `nessie` | Which catalog to use: `nessie` or `glue` |
| `MINIO_URL` | if nessie| — | MinIO S3 API base URL |
| `MINIO_ACCESS_KEY` | if nessie| — | MinIO access key |
| `MINIO_SECRET_KEY` | if nessie| — | MinIO secret key |
| `MINIO_BUCKET` | no | `iceberg` | Destination bucket |
| `NESSIE_URI` | if nessie| — | Nessie Iceberg REST catalog URI |
| `WAREHOUSE` | no | `s3://iceberg` | Iceberg warehouse path |
| `GLUE_CATALOG_ID` | no | — | AWS account ID for Glue Catalog |
| `SOCKET_PATH` | no | `/var/run/app/iceberg.sock` | UDS socket path |
| `ROCKSDB_PATH` | no | `/var/lib/rocksdb` | RocksDB data directory |
| `FLUSH_INTERVAL_SECS` | no | `600` | Base flush interval |
| `FLUSH_JITTER_SECS` | no | `120` | Max jitter (±) added to interval |

## Building and Deploying

```powershell
# Build the Docker image and push it to the local k3s registry
.\push.ps1
```

This is the only step needed when deploying to the k3s-on-Hyper-V home lab cluster.
`push.ps1` builds the image via Docker Desktop and pushes it to `k8s-node-1.local:5000`,
which the cluster pulls from when the `micewriter-k8s-injector` injects the sidecar.

```bash
# Native Rust build (requires Rust toolchain + C++ compiler + cmake for RocksDB)
cargo build --release

# Docker image only (no push)
docker build -t micewriter-engine:latest .
```

> **Note:** `cargo build` compiles RocksDB from C++ source on first run — expect 5–10 minutes. Subsequent builds use the cargo cache.

## Iceberg Dependency Versions

We use `iceberg-rust` v0.9+ for full native support of `fast_append` and FileIO operations without needing Python fallbacks.
