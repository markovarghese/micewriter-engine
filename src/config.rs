use std::env;
use anyhow::{Context, Result};
use crate::field_type::FieldDef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogType {
    Nessie,
    Glue,
}

pub struct Config {
    pub catalog_type: CatalogType,

    /// The specific Iceberg table this engine pipeline is pinned to.
    pub micewriter_table: String,

    /// Port for the gRPC server.
    pub grpc_port: u16,

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

    /// Number of frozen CFs to retain before enforcing backpressure. Default 3. 0 disables the limit.
    pub max_retained_frozen_cfs: usize,

    pub write_buffer_size: usize,
    pub concurrent_cf_flushes: usize,

    /// Target encoded byte size per Parquet file; the rolling writer starts a
    /// new file once written + in-progress bytes exceed this (default 64 MiB).
    pub target_parquet_bytes: usize,

    /// Target in-memory byte size per Parquet row group (default 16 MiB).
    /// parquet only exposes a row-count knob, so the flush pipeline derives a
    /// per-table row cap from this and the observed average record size. This
    /// is the primary bound on writer memory: a row group buffers entirely in
    /// RAM before it can flush to S3.
    pub parquet_row_group_bytes: usize,

    /// Parquet compression codec (default SNAPPY). Accepts NONE|SNAPPY|ZSTD.
    pub parquet_compression: parquet::basic::Compression,

    /// Field definitions loaded from schemas/<MICEWRITER_TABLE>.json at startup.
    /// Used for Iceberg table creation and schema validation.
    pub field_defs: Vec<FieldDef>,
}

fn load_field_defs(table: &str, schemas_dir: &str) -> Result<Vec<FieldDef>> {
    let path = std::path::Path::new(schemas_dir).join(format!("{}.json", table));
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read schema file {:?}", path))?;
    let schema: serde_json::Value = serde_json::from_str(&content)
        .context("Failed to parse schema JSON")?;
    let mut defs = Vec::new();
    if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
        for field in fields {
            let name = field["name"].as_str().unwrap_or("unknown").to_string();
            let field_type = field["type"].as_str().unwrap_or("string").to_string();
            let required = field["required"].as_bool().unwrap_or(false);
            defs.push(FieldDef { name, field_type, required });
        }
    }
    Ok(defs)
}

fn parse_parquet_compression(s: &str) -> anyhow::Result<parquet::basic::Compression> {
    use parquet::basic::{Compression, ZstdLevel};
    Ok(match s.trim().to_ascii_uppercase().as_str() {
        "NONE" | "UNCOMPRESSED" => Compression::UNCOMPRESSED,
        "SNAPPY"                => Compression::SNAPPY,
        "ZSTD"                  => Compression::ZSTD(ZstdLevel::default()),
        other => anyhow::bail!(
            "unsupported PARQUET_COMPRESSION '{}' (expected NONE|SNAPPY|ZSTD)", other
        ),
    })
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

        let micewriter_table = env::var("MICEWRITER_TABLE")
            .context("MICEWRITER_TABLE environment variable is strictly required in v2")?;
        let schemas_dir = env::var("SCHEMAS_DIR").unwrap_or_else(|_| "./schemas".to_string());
        let field_defs = load_field_defs(&micewriter_table, &schemas_dir)?;

        let mut config = Config {
            catalog_type,
            micewriter_table,
            grpc_port: env::var("GRPC_PORT")
                .unwrap_or_else(|_| "9090".to_string())
                .parse()
                .context("GRPC_PORT must be a valid u16 port number")?,
            minio_url,
            minio_access_key,
            minio_secret_key,
            nessie_uri,
            warehouse: env::var("WAREHOUSE")
                .or_else(|_| env::var("NESSIE_WAREHOUSE"))
                .unwrap_or_else(|_| "s3://iceberg".to_string()),
            glue_catalog_id: env::var("GLUE_CATALOG_ID").ok(),
            flush_interval_secs: env::var("FLUSH_INTERVAL_SECS")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .context("FLUSH_INTERVAL_SECS must be a number")?,
            flush_jitter_secs: env::var("FLUSH_JITTER_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .context("FLUSH_JITTER_SECS must be a number")?,
            flush_size_bytes: env::var("FLUSH_SIZE_BYTES")
                .unwrap_or_else(|_| "134217728".to_string())
                .parse()
                .context("FLUSH_SIZE_BYTES must be a number")?,
            flush_size_jitter_bytes: env::var("FLUSH_SIZE_JITTER_BYTES")
                .unwrap_or_else(|_| "67108864".to_string())
                .parse()
                .context("FLUSH_SIZE_JITTER_BYTES must be a number")?,
            enable_manual_flush: env::var("ENABLE_MANUAL_FLUSH")
                .map(|v| v.to_lowercase() != "false")
                .unwrap_or(true),
            rocksdb_path: env::var("ROCKSDB_PATH")
                .unwrap_or_else(|_| "/var/lib/rocksdb".to_string()),
            rocksdb_sync_writes: env::var("ROCKSDB_SYNC_WRITES")
                .map(|v| v.to_lowercase() != "false")
                .unwrap_or(true),
            max_retained_frozen_cfs: env::var("MAX_RETAINED_FROZEN_CFS")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .context("MAX_RETAINED_FROZEN_CFS must be an integer")?,
            write_buffer_size: env::var("WRITE_BUFFER_SIZE")
                .unwrap_or_else(|_| "67108864".to_string())
                .parse()
                .context("WRITE_BUFFER_SIZE must be a positive integer")?,
            concurrent_cf_flushes: env::var("CONCURRENT_CF_FLUSHES")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .context("CONCURRENT_CF_FLUSHES must be a positive integer")?,
            target_parquet_bytes: env::var("TARGET_PARQUET_BYTES")
                .unwrap_or_else(|_| "67108864".to_string())
                .parse()
                .context("TARGET_PARQUET_BYTES must be a positive integer")?,
            parquet_row_group_bytes: env::var("PARQUET_ROW_GROUP_BYTES")
                .unwrap_or_else(|_| "8388608".to_string())
                .parse()
                .context("PARQUET_ROW_GROUP_BYTES must be a positive integer")?,
            parquet_compression: parse_parquet_compression(
                &env::var("PARQUET_COMPRESSION").unwrap_or_else(|_| "SNAPPY".to_string()),
            )?,
            field_defs,
        };

        // 1. Read the exact memory limit injected by the pipeline. Default to 512 MiB.
        let mem_limit_bytes = env::var("ENGINE_MEM_LIMIT_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(512 * 1024 * 1024);

        // 2. Scale RocksDB write buffer size dynamically.
        // Cap at 64MB to prevent excessive memory usage.
        let parser_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1).max(1);
        config.write_buffer_size = (4 * 1024 * 1024 * parser_threads).min(64 * 1024 * 1024);

        Ok(config)
    }
}
