use lazy_static::lazy_static;
use prometheus::{Encoder, IntCounter, IntCounterVec, Opts, Registry, TextEncoder};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    pub static ref PARQUET_FILES_WRITTEN: IntCounter = IntCounter::new(
        "engine_parquet_files_written_total",
        "Total number of Parquet files uploaded to MinIO"
    )
    .expect("metric can be created");

    pub static ref PARQUET_BYTES_WRITTEN: IntCounter = IntCounter::new(
        "engine_parquet_bytes_written_total",
        "Total size of Parquet files uploaded to MinIO in bytes"
    )
    .expect("metric can be created");

    pub static ref CATALOG_COMMITS: IntCounter = IntCounter::new(
        "engine_catalog_commits_total",
        "Total number of successful Iceberg catalog commits"
    )
    .expect("metric can be created");

    pub static ref IPC_REQUESTS: IntCounterVec = IntCounterVec::new(
        Opts::new("engine_ipc_requests_total", "Total IPC requests received by engine"),
        &["type"]
    )
    .expect("metric can be created");

    pub static ref IPC_RESPONSES: IntCounterVec = IntCounterVec::new(
        Opts::new("engine_ipc_responses_total", "Total IPC responses sent by engine"),
        &["status"]
    )
    .expect("metric can be created");
}

pub fn register_custom_metrics() {
    REGISTRY
        .register(Box::new(PARQUET_FILES_WRITTEN.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(PARQUET_BYTES_WRITTEN.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(CATALOG_COMMITS.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(IPC_REQUESTS.clone()))
        .expect("collector can be registered");
    REGISTRY
        .register(Box::new(IPC_RESPONSES.clone()))
        .expect("collector can be registered");
}

pub fn gather_metrics() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
