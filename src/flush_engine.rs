use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rand::Rng;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::iceberg_writer;
use crate::parquet_writer;
use crate::protocol::RegisterSchema;
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

        if let Err(e) = do_flush(Arc::clone(&store), &registry, &config).await {
            error!("Flush cycle failed: {:#}", e);
            // Continue looping — a failed flush leaves data in the frozen CF which
            // will be picked up by the `drop_frozen_cf` guard. For now we just log
            // and wait for the next cycle.
        }
    }
}

/// Perform one full flush cycle: rotate → compile → upload → commit → purge.
pub async fn do_flush(
    store: Arc<RocksStore>,
    registry: &SchemaRegistry,
    config: &Config,
) -> Result<()> {
    info!("Starting flush cycle");

    let store_clone = Arc::clone(&store);
    let (frozen_cf, raw_records) = tokio::task::spawn_blocking(move || {
        store_clone.rotate()
    }).await??;

    if raw_records.is_empty() {
        info!("Nothing to flush");
        let store_clone2 = Arc::clone(&store);
        let cf = frozen_cf.clone();
        tokio::task::spawn_blocking(move || {
            store_clone2.drop_frozen_cf(&cf)
        }).await??;
        return Ok(());
    }

    // Group raw binary payloads by table name.
    let mut by_table: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    for bytes in raw_records {
        if bytes.len() < 2 {
            warn!("Skipping malformed record (too short for table name length)");
            continue;
        }
        let table_name_len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        if bytes.len() < 2 + table_name_len + 4 {
            warn!("Skipping malformed record (too short for table name and schema id)");
            continue;
        }
        
        let table_name_bytes = &bytes[2..2 + table_name_len];
        match std::str::from_utf8(table_name_bytes) {
            Ok(t) => by_table.entry(t.to_string()).or_default().push(bytes),
            Err(e) => warn!("Skipping malformed record (invalid utf-8 table name): {}", e),
        }
    }

    info!(tables = by_table.len(), "Flushing tables");

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
        let cf = frozen_cf.clone();
        tokio::task::spawn_blocking(move || {
            store.drop_frozen_cf(&cf)
        }).await??;
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
    records: &[Vec<u8>],
    schema: &RegisterSchema,
    config: &Config,
) -> Result<()> {
    // Parquet compilation blocks the thread. We should run it on spawn_blocking.
    let records_clone = records.to_vec();
    let fields_clone = schema.fields.clone();
    
    let parquet_bytes = tokio::task::spawn_blocking(move || {
        parquet_writer::compile(&records_clone, &fields_clone)
    }).await??;

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
