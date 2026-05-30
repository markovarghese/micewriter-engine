# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```powershell
# Build Docker image and push to local k3s registry (primary deployment workflow)
.\push.ps1

# Docker image only (no push)
docker build -t micewriter-engine:latest .

# Native Rust build — requires Rust toolchain + C++ compiler + cmake (for RocksDB)
# First build compiles RocksDB from C++ source: expect 5–10 minutes
cargo build --release

# Dev build
cargo build

# Lint / format
cargo clippy
cargo fmt
```

## Architecture

This is a **Kubernetes sidecar** in the [mIceWriter Ingestion Ecosystem](../micewriter-hub/README.md). It sits alongside a Java application, accepts telemetry records over a Unix Domain Socket, durably buffers them in a local RocksDB instance, and on a jittered ~10-minute cycle flushes them as Parquet files to an Apache Iceberg table.

```
Java SDK ──UDS──► uds_server.rs ──► rocksdb_store.rs (active CF)
                                              │
                                    (every ~10 min)
                                              │
                           flush_engine.rs   ▼
                              rotate CF ──► CBOR→Arrow→Parquet ──► iceberg_writer.rs
                                                                         │
                                                             MinIO S3 + Nessie/Glue commit
```

### IPC Protocol

All frames use a **4-byte big-endian length prefix** + 1-byte message type discriminant:

- `0x01` (`MSG_REGISTER_SCHEMA`): JSON `RegisterSchema` body — sent once per table on SDK startup. Stored in an in-memory `SchemaRegistry` (`Arc<RwLock<HashMap<String, RegisterSchema>>>`). **Lost on restart; SDK must re-register.**
- `0x02` (`MSG_INGEST_RECORD`): Custom binary frame — `[u16 table_name_len][table_name_bytes][CBOR bytes]`. The CBOR bytes are stored raw in RocksDB with no deserialization on the hot path.
- `0x03` (`MSG_FLUSH_NOW`): No body — triggers an immediate flush cycle. Only accepted when `ENABLE_MANUAL_FLUSH=true`; used for testing.
- ACK responses (Engine → SDK): 4-byte length prefix + JSON `AckResponse`.

### RocksDB Column Family Rotation

`rocksdb_store.rs` uses active/frozen column family rotation to implement lock-free flush batching:

1. On flush, `rotate()` creates a new `active_<timestamp>` CF and atomically switches it to be the active target, leaving the old CF frozen in place.
2. The frozen CF is drained and passed to the flush pipeline.
3. After a successful Iceberg commit, `drop_frozen_cf()` deletes it. If any table fails, the frozen CF is **retained** for manual inspection.

Records are keyed by a monotonically increasing 8-byte big-endian counter.

### Parquet / Iceberg Flush Pipeline

`flush_engine.rs` → `iceberg_writer.rs`:

- `flush_engine.rs` streams records from the frozen CF, strips the `[u16 table_name_len + table_name]` header, deserializes the CBOR payload via `ciborium` into JSON values, batches them into Arrow `RecordBatch`es using `arrow_json::ReaderBuilder` (schema derived from the registered `FieldDef` list), and writes a single in-memory Parquet file per table using `ArrowWriter`. Batches are flushed every 1 000 records to cap memory usage.
- `iceberg_writer::flush_table` creates the namespace/table if absent, derives the Iceberg schema from the registered `FieldDef` list, uploads the Parquet bytes via the table's `FileIO` abstraction (S3/MinIO), and commits using `fast_append` with up to 5 retries and exponential backoff (for optimistic locking conflicts).

### Catalog Support

Controlled by `CATALOG_TYPE` env var (`nessie` default, or `glue`):

- **Nessie**: REST catalog via `iceberg-catalog-rest`. Requires `MINIO_URL`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `NESSIE_URI`.
- **Glue**: AWS Glue catalog via `iceberg-catalog-glue`. Uses ambient AWS credentials; `GLUE_CATALOG_ID` is optional.

### Shutdown

SIGTERM (or Ctrl+C) stops the UDS server from accepting new connections, drains in-flight handlers, then triggers an emergency flush of whatever remains in the active CF before the process exits.
