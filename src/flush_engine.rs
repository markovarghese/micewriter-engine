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
    let mut next_id = 1;
    let arrow_fields = fields
        .iter()
        .map(|f| {
            let field_id = next_id;
            next_id += 1;
            let dt = MappedType::from_str_or_string(&f.field_type, &f.name).to_arrow(&mut next_id);
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            Field::new(&f.name, dt, !f.required).with_metadata(metadata)
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
    commit_tx: tokio::sync::mpsc::UnboundedSender<CommitRequest>,
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

        if let Err(e) = do_flush(Arc::clone(&store), &registry, &config, Arc::clone(&state), &commit_tx).await {
            error!("Flush cycle failed: {:#}", e);
        }
    }
}

pub struct CommitRequest {
    pub cf_name: String,
    pub table_data_files: HashMap<String, Vec<iceberg::spec::DataFile>>,
}

/// Batched Iceberg Commit loop.
pub async fn run_committer_loop(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<CommitRequest>,
    catalog: Arc<crate::iceberg_writer::CatalogHandle>,
    state: Arc<IcebergState>,
    registry: SchemaRegistry,
    store: Arc<RocksStore>,
) {
    loop {
        // Wait for at least one request
        let first_req = match rx.recv().await {
            Some(req) => req,
            None => break, // Channel closed (shutdown)
        };

        let mut batch = vec![first_req];
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }

        let mut all_data_files: HashMap<String, Vec<iceberg::spec::DataFile>> = HashMap::new();
        let mut cfs_to_drop = Vec::new();

        for req in batch {
            for (table, dfs) in req.table_data_files {
                all_data_files.entry(table).or_default().extend(dfs);
            }
            cfs_to_drop.push(req.cf_name);
        }

        let schemas = registry.read().unwrap().clone();
        let mut fully_committed = true;

        for (table_name, data_files) in all_data_files {
            if data_files.is_empty() { continue; }
            let schema = schemas.get(&table_name).unwrap();

            if let Err(e) = iceberg_writer::commit_data_files(
                &catalog,
                &state,
                &table_name,
                &schema.namespace,
                data_files,
                &schema.fields,
            ).await {
                error!(table = %table_name, "Batched table commit failed: {:#}", e);
                fully_committed = false;
            }
        }

        if fully_committed {
            for cf in cfs_to_drop {
                let store_clone = Arc::clone(&store);
                let cf_clone = cf.clone();
                if let Err(e) = tokio::task::spawn_blocking(move || store_clone.drop_frozen_cf(&cf_clone)).await {
                    error!(cf = %cf, "Failed to drop CF after batched commit: {:#}", e);
                } else {
                    info!(cf = %cf, "Dropped CF after batched commit");
                }
            }
        } else {
            for cf in cfs_to_drop {
                store.retain_frozen_cf(cf.clone());
            }
            warn!("Batched commit failed — frozen CFs retained for later recovery");
        }
    }
    info!("Iceberg committer loop exited cleanly");
}

/// Perform one full flush cycle: rotate → compile → upload → send to committer.
/// Returns a vector of JoinHandles so the caller can await the background processing tasks if needed.
pub async fn do_flush(
    store: Arc<RocksStore>,
    registry: &SchemaRegistry,
    config: &Config,
    state: Arc<IcebergState>,
    commit_tx: &tokio::sync::mpsc::UnboundedSender<CommitRequest>,
) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
    info!("Starting flush cycle");

    let mut cfs_to_flush = store.get_orphaned_cfs();
    let store_clone = Arc::clone(&store);
    let frozen_cf = tokio::task::spawn_blocking(move || {
        store_clone.rotate()
    }).await??;
    cfs_to_flush.push(frozen_cf);

    let catalog = match iceberg_writer::build_catalog(config).await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to build catalog: {:#}", e);
            return Err(e);
        }
    };
    let schemas = registry.read().unwrap().clone();

    let mut handles = Vec::new();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.concurrent_cf_flushes));

    for cf in cfs_to_flush {
        info!(cf = %cf, "Flushing CF in background task");
        let store_clone = Arc::clone(&store);
        let cf_clone = cf.clone();
        let batch_size = config.flush_compile_batch_size;
        let batch_bytes = config.flush_compile_batch_bytes;
        let parser_threads = config.parser_threads;
        let schemas_clone = schemas.clone();
        let catalog_clone = Arc::clone(&catalog);
        let state_clone = Arc::clone(&state);

        let commit_tx_clone = commit_tx.clone();
        let sem_clone = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let (upload_tx, mut upload_rx) = tokio::sync::mpsc::channel::<(String, Vec<u8>, u64)>(16);

            let compile_store = Arc::clone(&store_clone);
            let compile_cf = cf_clone.clone();
            let compile_schemas = schemas_clone.clone();

            let permit = sem_clone.acquire_owned().await.unwrap();
            
            // Spawn Stage 1, 2, 3 in a blocking thread (Reader, Parsers, Compressor)
            let compile_handle = tokio::task::spawn_blocking(move || {
                let _permit = permit; // Drop permit when CPU compilation finishes
                compile_cf_pipeline(
                    &compile_store,
                    &compile_cf,
                    &compile_schemas,
                    batch_size,
                    batch_bytes,
                    parser_threads,
                    upload_tx,
                )
            });
            // Stage 4: Uploader (Async I/O)
            let max_uploaders = parser_threads * 4; // Bounded concurrency based on CPU
            let mut upload_join_set = tokio::task::JoinSet::new();
            while let Some((table_name, parquet_bytes, row_count)) = upload_rx.recv().await {
                while upload_join_set.len() >= max_uploaders {
                    upload_join_set.join_next().await;
                }
                let schema = match schemas_clone.get(&table_name) {
                    Some(s) => s.clone(),
                    None => continue,
                };
                let upload_catalog = Arc::clone(&catalog_clone);
                let upload_state = state_clone.clone();

                upload_join_set.spawn(async move {
                    let res = iceberg_writer::upload_parquet_chunk(
                        &upload_catalog,
                        &upload_state,
                        &table_name,
                        &schema.namespace,
                        parquet_bytes,
                        row_count,
                        &schema.fields,
                    ).await;
                    (table_name, res)
                });
            }

            let mut table_data_files: HashMap<String, Vec<iceberg::spec::DataFile>> = HashMap::new();
            while let Some(res) = upload_join_set.join_next().await {
                if let Ok((table_name, upload_res)) = res {
                    match upload_res {
                        Ok(data_file) => table_data_files.entry(table_name).or_default().push(data_file),
                        Err(e) => error!(table = %table_name, "Failed to upload Parquet chunk: {:#}", e),
                    }
                }
            }

            // Wait for compilation to completely finish
            let compile_res = compile_handle.await.context("compile task panicked")?;
            let commit_all_ok = match compile_res {
                Ok(_) => true,
                Err(e) => {
                    error!(cf = %cf_clone, "Failed to compile CF, retaining for later: {:#}", e);
                    false
                }
            };

            // If compilation was successful, send data files to the committer queue
            if commit_all_ok {
                let req = CommitRequest {
                    cf_name: cf_clone.clone(),
                    table_data_files,
                };
                if let Err(e) = commit_tx_clone.send(req) {
                    error!(cf = %cf_clone, "Failed to send commit request to Iceberg committer: {}", e);
                    store_clone.retain_frozen_cf(cf_clone);
                }
            } else {
                store_clone.retain_frozen_cf(cf_clone.clone());
                warn!(cf = %cf_clone, retained = store_clone.retained_cf_count(), "Failed to compile CF — frozen CF retained for later recovery");
            }
            
            Ok(())
        });

        handles.push(handle);
    }

    Ok(handles)
}

fn compile_cf_pipeline(
    store: &RocksStore,
    cf_name: &str,
    schemas: &HashMap<String, crate::protocol::RegisterSchema>,
    batch_size: usize,
    batch_bytes: usize,
    parser_threads: usize,
    upload_tx: tokio::sync::mpsc::Sender<(String, Vec<u8>, u64)>,
) -> Result<()> {
    // Dynamic channel bounds based on thread count
    let queue_size = (16 / parser_threads).max(2);
    
    // Stage 1 & 2: Reader & Parsers
    let (chunk_tx, chunk_rx) = std::sync::mpsc::sync_channel::<(String, Vec<Vec<u8>>)>(queue_size);
    let (parsed_tx, parsed_rx) = std::sync::mpsc::sync_channel::<(String, Vec<arrow::record_batch::RecordBatch>)>(queue_size);
    let chunk_rx = Arc::new(std::sync::Mutex::new(chunk_rx));

    let mut parser_handles = Vec::new();
    for _ in 0..parser_threads {
        let chunk_rx_clone = Arc::clone(&chunk_rx);
        let parsed_tx_clone = parsed_tx.clone();
        let schemas_clone = schemas.clone();
        parser_handles.push(tokio::task::spawn_blocking(move || {
            loop {
                let (table_name, chunk) = match chunk_rx_clone.lock().unwrap().recv() {
                    Ok(msg) => msg,
                    Err(_) => break, // Channel closed
                };
                
                let schema_def = match schemas_clone.get(&table_name) {
                    Some(s) => s.clone(),
                    None => continue,
                };

                let arrow_schema = build_arrow_schema(&schema_def.fields);
                let mut buf = Vec::new();
                for json_bytes in chunk {
                    buf.extend_from_slice(&json_bytes);
                    buf.push(b'\n');
                }
                if buf.is_empty() { continue; }
                
                if let Ok(reader) = arrow_json::ReaderBuilder::new(arrow_schema).build(std::io::Cursor::new(buf)) {
                    let mut batches = Vec::new();
                    for b in reader {
                        if let Ok(batch) = b {
                            batches.push(batch);
                        }
                    }
                    if !batches.is_empty() {
                        let _ = parsed_tx_clone.send((table_name, batches));
                    }
                }
            }
        }));
    }

    std::thread::scope(|s| {
        s.spawn(|| {
            let mut raw_batches: HashMap<String, (usize, Vec<Vec<u8>>)> = HashMap::new();
            let _ = store.iterate_cf(cf_name, |record_bytes| {
                if record_bytes.len() < 2 { return Ok(()); }
                let table_name_len = u16::from_be_bytes([record_bytes[0], record_bytes[1]]) as usize;
                if record_bytes.len() < 2 + table_name_len { return Ok(()); }
                
                let table_name_bytes = &record_bytes[2..2 + table_name_len];
                let table_name = match std::str::from_utf8(table_name_bytes) {
                    Ok(t) => t.to_string(),
                    Err(_) => return Ok(()),
                };
                
                let json_bytes = &record_bytes[2 + table_name_len..];
                let json_len = json_bytes.len();
                let (current_bytes, json_vec) = raw_batches.entry(table_name.clone()).or_insert((0, Vec::new()));
                json_vec.push(json_bytes.to_vec());
                *current_bytes += json_len;

                if json_vec.len() >= batch_size || *current_bytes >= batch_bytes {
                    let chunk = std::mem::replace(json_vec, Vec::with_capacity(batch_size));
                    *current_bytes = 0;
                    let _ = chunk_tx.send((table_name.clone(), chunk));
                }
                Ok(())
            });

            // Drain remaining batches
            for (table_name, (_, chunk)) in raw_batches {
                if !chunk.is_empty() {
                    let _ = chunk_tx.send((table_name, chunk));
                }
            }
            drop(chunk_tx); // Signal parsers to stop
            drop(parsed_tx); // Will eventually close when parsers are done
        });

        // Stage 3: Compressor
        struct TableWriter {
            writer: parquet::arrow::ArrowWriter<Vec<u8>>,
            row_count: u64,
            schema: Arc<ArrowSchema>,
            props: Arc<parquet::file::properties::WriterProperties>,
        }
        
        let mut table_writers: HashMap<String, TableWriter> = HashMap::new();
        let props = Arc::new(parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build());

        while let Ok((table_name, batches)) = parsed_rx.recv() {
            for batch in batches {
                let num_rows = batch.num_rows() as u64;
                
                let tw = table_writers.entry(table_name.clone()).or_insert_with(|| {
                    let schema_def = schemas.get(&table_name).unwrap();
                    let arrow_schema = build_arrow_schema(&schema_def.fields);
                    TableWriter {
                        writer: parquet::arrow::ArrowWriter::try_new(vec![], arrow_schema.clone(), Some(props.as_ref().clone())).unwrap(),
                        row_count: 0,
                        schema: arrow_schema,
                        props: props.clone(),
                    }
                });
                
                let _ = tw.writer.write(&batch);
                tw.row_count += num_rows;
                
                // Pipeline condition: push a ~5MB Parquet chunk to the uploader
                if tw.row_count >= 25_000 {
                    let mut old_tw = table_writers.remove(&table_name).unwrap();
                    if let Ok(parquet_bytes) = old_tw.writer.into_inner() {
                        let _ = upload_tx.blocking_send((table_name.clone(), parquet_bytes, old_tw.row_count));
                    }
                }
            }
        }
        
        // Finalize any remaining writers
        for (table_name, tw) in table_writers {
            if tw.row_count > 0 {
                if let Ok(parquet_bytes) = tw.writer.into_inner() {
                    let _ = upload_tx.blocking_send((table_name, parquet_bytes, tw.row_count));
                }
            }
        }
    }); // end of std::thread::scope

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
