use std::env;
use anyhow::{Context, Result};

pub struct Config {
    /// Path to the Unix Domain Socket the UDS server listens on.
    pub socket_path: String,

    /// MinIO S3 API base URL (e.g. http://minio-api.local).
    pub minio_url: String,
    pub minio_access_key: String,
    pub minio_secret_key: String,
    pub minio_bucket: String,

    /// Nessie Iceberg REST Catalog URI (e.g. http://nessie.local/iceberg/v1).
    pub nessie_uri: String,
    /// Iceberg warehouse location prefix (e.g. s3://iceberg).
    pub nessie_warehouse: String,

    /// Base flush interval in seconds (default 600 = 10 minutes).
    pub flush_interval_secs: u64,
    /// Maximum random jitter added/subtracted from flush_interval_secs (default 120).
    pub flush_jitter_secs: u64,

    /// Directory for RocksDB files (should be on a dedicated PVC in k8s).
    pub rocksdb_path: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            socket_path: env::var("SOCKET_PATH")
                .unwrap_or_else(|_| "/var/run/app/iceberg.sock".to_string()),
            minio_url: env::var("MINIO_URL").context("MINIO_URL required")?,
            minio_access_key: env::var("MINIO_ACCESS_KEY").context("MINIO_ACCESS_KEY required")?,
            minio_secret_key: env::var("MINIO_SECRET_KEY").context("MINIO_SECRET_KEY required")?,
            minio_bucket: env::var("MINIO_BUCKET").unwrap_or_else(|_| "iceberg".to_string()),
            nessie_uri: env::var("NESSIE_URI").context("NESSIE_URI required")?,
            nessie_warehouse: env::var("NESSIE_WAREHOUSE")
                .unwrap_or_else(|_| "s3://iceberg".to_string()),
            flush_interval_secs: env::var("FLUSH_INTERVAL_SECS")
                .unwrap_or_else(|_| "600".to_string())
                .parse()
                .context("FLUSH_INTERVAL_SECS must be a number")?,
            flush_jitter_secs: env::var("FLUSH_JITTER_SECS")
                .unwrap_or_else(|_| "120".to_string())
                .parse()
                .context("FLUSH_JITTER_SECS must be a number")?,
            rocksdb_path: env::var("ROCKSDB_PATH")
                .unwrap_or_else(|_| "/var/lib/rocksdb".to_string()),
        })
    }
}
