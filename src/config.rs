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

    /// If true, the UDS server will accept MSG_FLUSH_NOW (0x03) from the SDK to force an immediate flush.
    pub enable_manual_flush: bool,

    /// Directory for RocksDB files (should be on a dedicated PVC in k8s).
    pub rocksdb_path: String,
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

        Ok(Self {
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
            enable_manual_flush: env::var("ENABLE_MANUAL_FLUSH")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
            rocksdb_path: env::var("ROCKSDB_PATH")
                .unwrap_or_else(|_| "/var/lib/rocksdb".to_string()),
        })
    }
}
