use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rand::Rng;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::field_type::MappedType;
use crate::iceberg_writer::{self, IcebergState};
use crate::protocol::FieldDef;
use crate::rocksdb_store::RocksStore;
use crate::uds_server::SchemaRegistry;
use arrow::datatypes::{Field, Schema as ArrowSchema};

fn build_arrow_schema(fields: &[FieldDef]) -> Arc<ArrowSchema> {
    let arrow_fields = fields
        .iter()
        .map(|f| {
            let dt = MappedType::from_str_or_string(&f.field_type, &f.name).to_arrow();
            Field::new(&f.name, dt, !f.required)
        })
        .collect::<Vec<_>>();
    Arc::new(ArrowSchema::new(arrow_fields))
}

/// Background task: sleeps for a jittered interval, then rotates the active
/// RocksDB column family and flushes all frozen records to Iceberg.
///
/// Exits when `shutdown` flips to `true`. The caller is responsible for
/// running the emergency flush after this returns.
pub async fn run_flush_loop(
    store: Arc<RocksStore>,
    registry: SchemaRegistry,
    config: Arc<Config>,
    state: Arc<IcebergState>,
    flush_trigger: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let sleep_secs = jittered_interval(&config);
        info!(secs = sleep_secs, "Next flush scheduled");

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(sleep_secs)) => {
                info!("Timer triggered flush");
            }
            _ = flush_trigger.notified() => {
                info!("Manual flush triggered via IPC");
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Flush loop received shutdown signal");
                    return;
                }
                continue;
            }
        }

        // If shutdown fires during a flush, still let the flush finish — partial
        // Iceberg state is worse than a few extra seconds of shutdown latency.
        if let Err(e) = do_flush(Arc::clone(&store), &registry, &config, &state).await {
            error!("Flush cycle failed: {:#}", e);
        }
    }
}

/// Perform one full flush cycle: rotate → compile → upload → commit → purge.
pub async fn do_flush(
    store: Arc<RocksStore>,
    registry: &SchemaRegistry,
    config: &Config,
    state: &IcebergState,
) -> Result<()> {
    info!("Starting flush cycle");

    let mut cfs_to_flush = store.get_orphaned_cfs();
    let store_clone = Arc::clone(&store);
    let frozen_cf = tokio::task::spawn_blocking(move || {
        store_clone.rotate()
    }).await??;
    cfs_to_flush.push(frozen_cf);

    let catalog = iceberg_writer::build_catalog(config).await?;
    let schemas = registry.read().unwrap().clone();

    for cf in cfs_to_flush {
        info!(cf = %cf, "Flushing CF");
        let store_clone = Arc::clone(&store);
        let cf_clone = cf.clone();
        let schemas_clone = schemas.clone();

        // Stream records, parsing CBOR to JSON values and batching them to
        // ArrowWriter to prevent memory spikes. Any conversion error inside
        // the blocking task aborts the flush for this CF so the records stay
        // in the frozen CF and can be retried on the next cycle.
        let batch_size = config.flush_compile_batch_size;
        let compile_res: Result<HashMap<String, (Vec<u8>, u64)>> =
            tokio::task::spawn_blocking(move || {
                compile_cf(&store_clone, &cf_clone, &schemas_clone, batch_size)
            })
            .await
            .context("compile task panicked")?;

        let results = match compile_res {
            Ok(r) => r,
            Err(e) => {
                error!(cf = %cf, "Failed to compile CF, retaining for later: {:#}", e);
                continue;
            }
        };

        if results.is_empty() {
            info!(cf = %cf, "Nothing to flush");
            let store_clone = Arc::clone(&store);
            let cf_clone = cf.clone();
            tokio::task::spawn_blocking(move || store_clone.drop_frozen_cf(&cf_clone)).await??;
            continue;
        }

        let mut commit_all_ok = true;
        for (table_name, (parquet_bytes, row_count)) in results {
            let schema = match schemas.get(&table_name) {
                Some(s) => s,
                None => {
                    warn!(table = %table_name, "No schema registered, skipping table");
                    commit_all_ok = false;
                    continue;
                }
            };

            match iceberg_writer::flush_table(
                &catalog,
                state,
                &table_name,
                &schema.namespace,
                parquet_bytes,
                row_count,
                &schema.fields,
            ).await {
                Ok(_) => info!(table = %table_name, rows = row_count, "Table flushed"),
                Err(e) => {
                    error!(table = %table_name, "Table flush failed: {:#}", e);
                    commit_all_ok = false;
                }
            }
        }

        if commit_all_ok {
            let store_clone = Arc::clone(&store);
            let cf_clone = cf.clone();
            tokio::task::spawn_blocking(move || store_clone.drop_frozen_cf(&cf_clone)).await??;
        } else {
            warn!(cf = %cf, "Some tables failed — frozen CF retained for later recovery");
        }
    }

    Ok(())
}

/// Compile every record in `cf_name` into per-table Parquet bytes and row counts.
/// Runs inside `spawn_blocking` because it does synchronous RocksDB and Parquet IO.
fn compile_cf(
    store: &RocksStore,
    cf_name: &str,
    schemas: &HashMap<String, crate::protocol::RegisterSchema>,
    batch_size: usize,
) -> Result<HashMap<String, (Vec<u8>, u64)>> {
    let mut writers: HashMap<String, parquet::arrow::ArrowWriter<Vec<u8>>> = HashMap::new();
    let mut buffers: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    let mut row_counts: HashMap<String, u64> = HashMap::new();
    let props = parquet::file::properties::WriterProperties::builder().build();

    let flush_buffer = |table_name: &str,
                        buf: &mut Vec<serde_json::Value>,
                        writers: &mut HashMap<String, parquet::arrow::ArrowWriter<Vec<u8>>>,
                        row_counts: &mut HashMap<String, u64>|
     -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let schema_def = match schemas.get(table_name) {
            Some(s) => s,
            None => {
                warn!(table = %table_name, "Dropping records — no schema registered");
                buf.clear();
                return Ok(());
            }
        };
        let arrow_schema = build_arrow_schema(&schema_def.fields);

        if !writers.contains_key(table_name) {
            let w = parquet::arrow::ArrowWriter::try_new(
                vec![],
                arrow_schema.clone(),
                Some(props.clone()),
            )
            .with_context(|| format!("ArrowWriter::try_new failed for '{}'", table_name))?;
            writers.insert(table_name.to_string(), w);
        }
        let writer = writers.get_mut(table_name).unwrap();

        // arrow-json expects NDJSON (one object per line), not a JSON array.
        let mut json_bytes: Vec<u8> = Vec::with_capacity(buf.len() * 128);
        for value in buf.iter() {
            serde_json::to_writer(&mut json_bytes, value)
                .with_context(|| format!("serde_json::to_writer failed for '{}'", table_name))?;
            json_bytes.push(b'\n');
        }
        let reader = arrow_json::ReaderBuilder::new(arrow_schema)
            .build(std::io::Cursor::new(json_bytes))
            .with_context(|| format!("arrow_json::ReaderBuilder failed for '{}'", table_name))?;

        for batch_result in reader {
            let batch = batch_result
                .with_context(|| format!("arrow_json batch read failed for '{}'", table_name))?;
            let rows = batch.num_rows() as u64;
            writer
                .write(&batch)
                .with_context(|| format!("ArrowWriter::write failed for '{}'", table_name))?;
            *row_counts.entry(table_name.to_string()).or_insert(0) += rows;
        }
        buf.clear();
        Ok(())
    };

    // iterate_cf takes a callback returning anyhow::Result; if flush_buffer fails
    // we propagate the real error through it directly.
    store
        .iterate_cf(cf_name, |record_bytes| {
            if record_bytes.len() < 2 {
                return Ok(());
            }
            let table_name_len = u16::from_be_bytes([record_bytes[0], record_bytes[1]]) as usize;
            if record_bytes.len() < 2 + table_name_len {
                return Ok(());
            }

            let table_name_bytes = &record_bytes[2..2 + table_name_len];
            let table_name = match std::str::from_utf8(table_name_bytes) {
                Ok(t) => t.to_string(),
                Err(_) => return Ok(()),
            };

            let cbor_bytes = &record_bytes[2 + table_name_len..];
            let value: serde_json::Value =
                match ciborium::de::from_reader(std::io::Cursor::new(cbor_bytes)) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(table = %table_name, "Skipping malformed CBOR record: {}", e);
                        return Ok(());
                    }
                };

            let buf = buffers.entry(table_name.clone()).or_default();
            buf.push(value);
            if buf.len() >= batch_size {
                flush_buffer(&table_name, buf, &mut writers, &mut row_counts)?;
            }
            Ok(())
        })
        .with_context(|| format!("iterate_cf '{}' failed", cf_name))?;

    // Drain remaining partial buffers.
    let table_names: Vec<String> = buffers.keys().cloned().collect();
    for table_name in table_names {
        let mut buf = buffers.remove(&table_name).unwrap();
        flush_buffer(&table_name, &mut buf, &mut writers, &mut row_counts)?;
    }

    let mut out = HashMap::new();
    for (table_name, writer) in writers {
        let bytes = writer
            .into_inner()
            .with_context(|| format!("ArrowWriter::into_inner failed for '{}'", table_name))?;
        let rows = row_counts.remove(&table_name).unwrap_or(0);
        out.insert(table_name, (bytes, rows));
    }
    Ok(out)
}

fn jittered_interval(config: &Config) -> u64 {
    let jitter = rand::thread_rng().gen_range(0..=config.flush_jitter_secs * 2);
    let secs = config
        .flush_interval_secs
        .saturating_add(jitter)
        .saturating_sub(config.flush_jitter_secs);
    secs.max(60)
}
