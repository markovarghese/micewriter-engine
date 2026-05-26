use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rand::Rng;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::iceberg_writer;
use crate::parquet_writer;
use crate::protocol::{IngestRecord, RegisterSchema};
use crate::rocksdb_store::RocksStore;
use crate::uds_server::SchemaRegistry;

/// Background task: sleeps for a jittered interval, then rotates the active
/// RocksDB column family and flushes all frozen records to Iceberg.
///
/// This loop runs indefinitely until the process exits (SIGTERM or Ctrl+C
/// triggers an emergency flush in `main.rs` first).
pub async fn run_flush_loop(
    store: Arc<RocksStore>,
    registry: SchemaRegistry,
    config: Arc<Config>,
) {
    loop {
        let sleep_secs = jittered_interval(&config);
        info!(secs = sleep_secs, "Next flush scheduled");
        tokio::time::sleep(Duration::from_secs(sleep_secs)).await;

        if let Err(e) = do_flush(&store, &registry, &config).await {
            error!("Flush cycle failed: {:#}", e);
            // Continue looping — a failed flush leaves data in the frozen CF which
            // will be picked up by the `drop_frozen_cf` guard. For now we just log
            // and wait for the next cycle.
        }
    }
}

/// Perform one full flush cycle: rotate → compile → upload → commit → purge.
pub async fn do_flush(
    store: &RocksStore,
    registry: &SchemaRegistry,
    config: &Config,
) -> Result<()> {
    info!("Starting flush cycle");

    let (frozen_cf, raw_records) = store.rotate()?;

    if raw_records.is_empty() {
        info!("Nothing to flush");
        store.drop_frozen_cf(&frozen_cf)?;
        return Ok(());
    }

    // Deserialize raw bytes back to IngestRecords and group by table name.
    let mut by_table: HashMap<String, Vec<IngestRecord>> = HashMap::new();
    for bytes in &raw_records {
        match serde_json::from_slice::<IngestRecord>(bytes) {
            Ok(r) => by_table.entry(r.table.clone()).or_default().push(r),
            Err(e) => warn!("Skipping malformed record: {}", e),
        }
    }

    info!(tables = by_table.len(), records = raw_records.len(), "Flushing tables");

    // Compile and commit each table independently.
    let schemas = registry.read().unwrap().clone();

    let mut all_ok = true;
    for (table_name, records) in &by_table {
        let schema = match schemas.get(table_name) {
            Some(s) => s,
            None => {
                warn!(table = %table_name, "No schema registered, skipping table");
                all_ok = false;
                continue;
            }
        };

        match compile_and_commit(table_name, records, schema, config).await {
            Ok(_) => info!(table = %table_name, "Table flushed"),
            Err(e) => {
                error!(table = %table_name, "Table flush failed: {:#}", e);
                all_ok = false;
            }
        }
    }

    if all_ok {
        store.drop_frozen_cf(&frozen_cf)?;
    } else {
        warn!(
            cf = %frozen_cf,
            "Some tables failed — frozen CF retained for manual inspection"
        );
    }

    Ok(())
}

async fn compile_and_commit(
    table_name: &str,
    records: &[IngestRecord],
    schema: &RegisterSchema,
    config: &Config,
) -> Result<()> {
    let parquet_bytes = parquet_writer::compile(records, &schema.fields)?;

    iceberg_writer::flush_table(
        table_name,
        &schema.namespace,
        parquet_bytes,
        &schema.fields,
        config,
    )
    .await
}

fn jittered_interval(config: &Config) -> u64 {
    let jitter = rand::thread_rng().gen_range(0..=config.flush_jitter_secs * 2);
    // Avoid underflow: base + jitter - max_jitter, clamped to at least 60s.
    let secs = config.flush_interval_secs.saturating_add(jitter).saturating_sub(config.flush_jitter_secs);
    secs.max(60)
}
