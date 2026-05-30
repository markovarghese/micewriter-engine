use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rand::Rng;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::iceberg_writer::{self, IcebergState};
use crate::protocol::FieldDef;
use crate::rocksdb_store::RocksStore;
use crate::uds_server::SchemaRegistry;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

fn build_arrow_schema(fields: &[FieldDef]) -> Arc<ArrowSchema> {
    let arrow_fields = fields.iter().map(|f| {
        let dt = match f.field_type.as_str() {
            "string" => DataType::Utf8,
            "long" => DataType::Int64,
            "int" => DataType::Int32,
            "double" => DataType::Float64,
            "float" => DataType::Float32,
            "boolean" => DataType::Boolean,
            "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "date" => DataType::Date32,
            "binary" => DataType::Binary,
            _ => DataType::Utf8,
        };
        Field::new(&f.name, dt, !f.required)
    }).collect::<Vec<_>>();
    Arc::new(ArrowSchema::new(arrow_fields))
}

/// Background task: sleeps for a jittered interval, then rotates the active
/// RocksDB column family and flushes all frozen records to Iceberg.
pub async fn run_flush_loop(
    store: Arc<RocksStore>,
    registry: SchemaRegistry,
    config: Arc<Config>,
    state: Arc<IcebergState>,
    flush_trigger: Arc<tokio::sync::Notify>,
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
        }

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
        
        // Stream records, parsing CBOR to JSON values and batching them to ArrowWriter to prevent memory spikes
        let (read_ok, results) = tokio::task::spawn_blocking(move || {
            let mut writers: HashMap<String, parquet::arrow::ArrowWriter<Vec<u8>>> = HashMap::new();
            let mut buffers: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
            let props = parquet::file::properties::WriterProperties::builder().build();
            
            let mut flush_buffer = |table_name: &str, buf: &mut Vec<serde_json::Value>| {
                if buf.is_empty() { return Ok(()); }
                if let Some(schema_def) = schemas_clone.get(table_name) {
                    let arrow_schema = build_arrow_schema(&schema_def.fields);
                    let writer = writers.entry(table_name.to_string()).or_insert_with(|| {
                        parquet::arrow::ArrowWriter::try_new(vec![], arrow_schema.clone(), Some(props.clone())).unwrap()
                    });
                    
                    let json_bytes = serde_json::to_vec(buf).unwrap();
                    let mut reader = arrow_json::ReaderBuilder::new(arrow_schema)
                        .build(std::io::Cursor::new(json_bytes))
                        .unwrap();
                        
                    while let Some(batch) = reader.next() {
                        if let Ok(b) = batch {
                            let _ = writer.write(&b);
                        }
                    }
                }
                buf.clear();
                Ok::<(), anyhow::Error>(())
            };
            
            let iterate_res = store_clone.iterate_cf(&cf_clone, |record_bytes| {
                if record_bytes.len() < 2 { return Ok(()); }
                let table_name_len = u16::from_be_bytes([record_bytes[0], record_bytes[1]]) as usize;
                if record_bytes.len() < 2 + table_name_len { return Ok(()); }
                
                let table_name_bytes = &record_bytes[2..2 + table_name_len];
                let table_name = match std::str::from_utf8(table_name_bytes) {
                    Ok(t) => t.to_string(),
                    Err(_) => return Ok(()),
                };
                
                let cbor_bytes = &record_bytes[2 + table_name_len..];
                if let Ok(value) = ciborium::de::from_reader::<serde_json::Value, _>(std::io::Cursor::new(cbor_bytes)) {
                    let buf = buffers.entry(table_name.clone()).or_default();
                    buf.push(value);
                    if buf.len() >= 1000 {
                        let _ = flush_buffer(&table_name, buf);
                    }
                }
                
                Ok(())
            });
            
            for (table_name, mut buf) in buffers {
                let _ = flush_buffer(&table_name, &mut buf);
            }
            
            if iterate_res.is_err() {
                return (false, HashMap::new());
            }
            
            let mut finished_bytes = HashMap::new();
            for (table_name, writer) in writers {
                if let Ok(bytes) = writer.into_inner() {
                    finished_bytes.insert(table_name, bytes);
                }
            }
            (true, finished_bytes)
        }).await?;

        if !read_ok {
            warn!(cf = %cf, "Failed to read CF, retaining for later");
            continue;
        }

        if results.is_empty() {
            info!(cf = %cf, "Nothing to flush");
            let store_clone = Arc::clone(&store);
            let cf_clone = cf.clone();
            tokio::task::spawn_blocking(move || store_clone.drop_frozen_cf(&cf_clone)).await??;
            continue;
        }

        let mut commit_all_ok = true;
        for (table_name, parquet_bytes) in results {
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
                &schema.fields,
                config,
            ).await {
                Ok(_) => info!(table = %table_name, "Table flushed"),
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

fn jittered_interval(config: &Config) -> u64 {
    let jitter = rand::thread_rng().gen_range(0..=config.flush_jitter_secs * 2);
    let secs = config
        .flush_interval_secs
        .saturating_add(jitter)
        .saturating_sub(config.flush_jitter_secs);
    secs.max(60)
}
