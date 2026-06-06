use std::env;
use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogType {
    Nessie,
    Glue,
}

pub struct Config {
    pub catalog_type: CatalogType,

    /// Path to the Unix Domain Socket the UDS server listens on.
    pub socket_path: String,

    // MinIO / Nessie specific properties (required if catalog_type == Nessie)
    pub minio_url: Option<String>,
    pub minio_access_key: Option<String>,
    pub minio_secret_key: Option<String>,
    pub nessie_uri: Option<String>,

    /// Iceberg warehouse location prefix (e.g. s3://iceberg)
    pub warehouse: String,

    // AWS Glue specific properties
    pub glue_catalog_id: Option<String>,

    /// Base flush interval in seconds (default 600 = 10 minutes).
    pub flush_interval_secs: u64,
    /// Maximum random jitter added/subtracted from flush_interval_secs (default 120).
    pub flush_jitter_secs: u64,

    /// Size limit in bytes for the active RocksDB column family before triggering a flush (default 192 MB).
    pub flush_size_bytes: u64,
    /// Maximum random jitter added/subtracted from flush_size_bytes (default 8 MB).
    pub flush_size_jitter_bytes: u64,

    /// If true, the UDS server will accept MSG_FLUSH_NOW (0x03) from the SDK to force an immediate flush.
    pub enable_manual_flush: bool,

    /// Directory for RocksDB files (should be on a dedicated PVC in k8s).
    pub rocksdb_path: String,

    /// If true, RocksDB writes use `sync=true` so each batched commit is fsync'd
    /// before the SDK receives an ACK. Default true — required to honour the
    /// "durable local buffer" contract.
    pub rocksdb_sync_writes: bool,

    /// How many records compile_cf buffers per table before flushing the
    /// batch through JSON→Arrow→Parquet. Larger values trade memory for
    /// fewer arrow_json invocations. Default 1000.
    pub flush_compile_batch_size: usize,

    /// Maximum byte size of uncompressed CBOR records to buffer per table before
    /// forcing an early flush to Parquet during compilation to bound memory. Default 4 MB.
    pub flush_compile_batch_bytes: usize,

    /// Number of frozen CFs to retain before enforcing backpressure. Default 3. 0 disables the limit.
    pub max_retained_frozen_cfs: usize,

    /// Number of parallel parser threads to spawn for compiling JSON to Arrow.
    /// Defaults to the number of available CPUs (respecting container limits).
    pub parser_threads: usize,

    pub write_buffer_size: usize,
    pub concurrent_cf_flushes: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let catalog_type = match env::var("CATALOG_TYPE").unwrap_or_else(|_| "nessie".to_string()).to_lowercase().as_str() {
            "glue" => CatalogType::Glue,
            _ => CatalogType::Nessie,
        };

        let minio_url = env::var("MINIO_URL").ok();
        let minio_access_key = env::var("MINIO_ACCESS_KEY").ok();
        let minio_secret_key = env::var("MINIO_SECRET_KEY").ok();
        let nessie_uri = env::var("NESSIE_URI").ok();

        if catalog_type == CatalogType::Nessie {
            if minio_url.is_none() || minio_access_key.is_none() || minio_secret_key.is_none() || nessie_uri.is_none() {
                anyhow::bail!("MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, and NESSIE_URI are required when CATALOG_TYPE=nessie");
            }
        }

        let mut config = Config {
            catalog_type,
            socket_path: env::var("SOCKET_PATH")
                .unwrap_or_else(|_| "/var/run/app/iceberg.sock".to_string()),
            minio_url,
            minio_access_key,
            minio_secret_key,
            nessie_uri,
            warehouse: env::var("WAREHOUSE")
                .or_else(|_| env::var("NESSIE_WAREHOUSE"))
                .unwrap_or_else(|_| "s3://iceberg".to_string()),
            glue_catalog_id: env::var("GLUE_CATALOG_ID").ok(),
            flush_interval_secs: env::var("FLUSH_INTERVAL_SECS")
                .unwrap_or_else(|_| "600".to_string())
                .parse()
                .context("FLUSH_INTERVAL_SECS must be a number")?,
            flush_jitter_secs: env::var("FLUSH_JITTER_SECS")
                .unwrap_or_else(|_| "120".to_string())
                .parse()
                .context("FLUSH_JITTER_SECS must be a number")?,
            flush_size_bytes: env::var("FLUSH_SIZE_BYTES")
                .unwrap_or_else(|_| "33554432".to_string())
                .parse()
                .context("FLUSH_SIZE_BYTES must be a number")?,
            flush_size_jitter_bytes: env::var("FLUSH_SIZE_JITTER_BYTES")
                .unwrap_or_else(|_| "8388608".to_string())
                .parse()
                .context("FLUSH_SIZE_JITTER_BYTES must be a number")?,
            enable_manual_flush: env::var("ENABLE_MANUAL_FLUSH")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
            rocksdb_path: env::var("ROCKSDB_PATH")
                .unwrap_or_else(|_| "/var/lib/rocksdb".to_string()),
            rocksdb_sync_writes: env::var("ROCKSDB_SYNC_WRITES")
                .map(|v| v.to_lowercase() != "false")
                .unwrap_or(true),
            flush_compile_batch_size: env::var("FLUSH_COMPILE_BATCH_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .context("FLUSH_COMPILE_BATCH_SIZE must be a positive integer")
                .and_then(|n: usize| {
                    if n == 0 {
                        anyhow::bail!("FLUSH_COMPILE_BATCH_SIZE must be > 0");
                    }
                    Ok(n)
                })?,
            flush_compile_batch_bytes: env::var("FLUSH_COMPILE_BATCH_BYTES")
                .unwrap_or_else(|_| "4194304".to_string())
                .parse()
                .context("FLUSH_COMPILE_BATCH_BYTES must be a positive integer")?,
            max_retained_frozen_cfs: env::var("MAX_RETAINED_FROZEN_CFS")
                .unwrap_or_else(|_| "8".to_string())
                .parse()
                .context("MAX_RETAINED_FROZEN_CFS must be an integer")?,
            parser_threads: env::var("PARSER_THREADS")
                .map(|s| s.parse().context("PARSER_THREADS must be a positive integer"))
                .unwrap_or_else(|_| Ok(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).max(1)))?,
            write_buffer_size: 4 * 1024 * 1024,
            concurrent_cf_flushes: 1,
        };

        // 1. Read the exact memory limit injected by the sidecar webhook. Default to 512 MiB.
        let mem_limit_bytes = env::var("ENGINE_MEM_LIMIT_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(512 * 1024 * 1024);

        // 2. Scale flush_compile_batch_bytes dynamically.
        // We budget ~1% of total memory for raw strings, which expands to ~10% under arrow_json overhead.
        let raw_string_budget = mem_limit_bytes / 100;
        config.flush_compile_batch_bytes = (raw_string_budget / config.parser_threads as u64) as usize;
        config.flush_compile_batch_bytes = config.flush_compile_batch_bytes.max(256 * 1024); // 256KB floor

        // 3. Scale concurrent CF flushes. 
        // We budget 1 CF pipeline per 256MB of RAM.
        config.concurrent_cf_flushes = (mem_limit_bytes / (256 * 1024 * 1024)).max(1) as usize;

        // 4. Scale RocksDB write buffer size dynamically.
        // Base is 4MB. Scale up by parser threads (more throughput = bigger buffers needed)
        // Cap at 64MB to prevent excessive memory usage.
        config.write_buffer_size = (4 * 1024 * 1024 * config.parser_threads).min(64 * 1024 * 1024);

        Ok(config)
    }
}
