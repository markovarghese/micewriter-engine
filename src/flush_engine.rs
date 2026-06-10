use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use iceberg::spec::DataFileFormat;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use rand::Rng;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::iceberg_writer::{self, IcebergState};
use crate::metrics;
use crate::rocksdb_store::RocksStore;
use crate::uds_server::SchemaRegistry;

type IcebergTableWriter = iceberg::writer::base_writer::data_file_writer::DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

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
    flush_semaphore: Arc<tokio::sync::Semaphore>,
) {
    loop {
        let sleep_secs = jittered_interval(&config);
        info!(secs = sleep_secs, "Next flush scheduled");

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(sleep_secs)) => {
                info!("Timer triggered flush");
            }
            _ = flush_trigger.notified() => {
                info!("IPC flush triggered (size limit or manual)");
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Flush loop received shutdown signal");
                    return;
                }
                continue;
            }
        }

        if let Err(e) = do_flush(Arc::clone(&store), &registry, &config, Arc::clone(&state), &commit_tx, Arc::clone(&flush_semaphore)).await {
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
            // A panic here would silently kill the committer loop, so degrade to
            // a retained-CF retry instead if the schema has somehow vanished.
            let Some(schema) = schemas.get(&table_name) else {
                error!(table = %table_name, "No registered schema at commit time — retaining CFs");
                fully_committed = false;
                continue;
            };

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

/// Perform one full flush cycle: rotate → pre-fetch table contexts → stream to S3 → send to committer.
pub async fn do_flush(
    store: Arc<RocksStore>,
    registry: &SchemaRegistry,
    config: &Config,
    state: Arc<IcebergState>,
    commit_tx: &tokio::sync::mpsc::UnboundedSender<CommitRequest>,
    semaphore: Arc<tokio::sync::Semaphore>,
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

    // Pre-fetch table write contexts (FileIO + location + Iceberg schema) for all known schemas.
    // This resolves/creates the Iceberg table metadata once, before entering
    // the blocking compile phase where we can't make async catalog calls.
    let mut table_write_contexts: HashMap<
        String,
        (iceberg::io::FileIO, String, iceberg::spec::SchemaRef),
    > = HashMap::new();
    for (table_name, schema) in &schemas {
        match iceberg_writer::get_table_write_context(
            &catalog, &state, table_name, &schema.namespace, &schema.fields,
        ).await {
            Ok(ctx) => { table_write_contexts.insert(table_name.clone(), ctx); }
            Err(e) => { error!(table = %table_name, "Failed to resolve table write context: {:#}", e); }
        }
    }

    let rt_handle = tokio::runtime::Handle::current();
    let table_write_contexts = Arc::new(table_write_contexts);

    let mut handles = Vec::new();

    for cf in cfs_to_flush {
        info!(cf = %cf, "Flushing CF in background task");
        let store_clone = Arc::clone(&store);
        let cf_clone = cf.clone();
        let batch_size = config.flush_compile_batch_size;
        let batch_bytes = config.flush_compile_batch_bytes;
        let parser_threads = config.parser_threads;
        let target_parquet_bytes = config.target_parquet_bytes;
        let parquet_row_group_bytes = config.parquet_row_group_bytes;
        let parquet_compression = config.parquet_compression;
        let table_write_contexts_clone = Arc::clone(&table_write_contexts);
        let rt_handle_clone = rt_handle.clone();

        let commit_tx_clone = commit_tx.clone();
        let sem_clone = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let compile_store = Arc::clone(&store_clone);
            let compile_cf = cf_clone.clone();

            let permit = sem_clone.acquire_owned().await.unwrap();

            // Stages 1–4 all run in a single spawn_blocking: reader, parser pool,
            // compressor, and streaming S3 upload (via rt_handle.block_on).
            let compile_handle = tokio::task::spawn_blocking(move || {
                let _permit = permit; // Hold semaphore for entire compile+upload duration
                compile_cf_pipeline(
                    &compile_store,
                    &compile_cf,
                    batch_size,
                    batch_bytes,
                    parser_threads,
                    target_parquet_bytes,
                    parquet_row_group_bytes,
                    parquet_compression,
                    &table_write_contexts_clone,
                    rt_handle_clone,
                )
            });

            match compile_handle.await.context("compile task panicked")? {
                Ok(table_data_files) => {
                    let req = CommitRequest {
                        cf_name: cf_clone.clone(),
                        table_data_files,
                    };
                    if let Err(e) = commit_tx_clone.send(req) {
                        error!(cf = %cf_clone, "Failed to send commit request to Iceberg committer: {}", e);
                        store_clone.retain_frozen_cf(cf_clone);
                    }
                }
                Err(e) => {
                    error!(cf = %cf_clone, "Compile+upload failed, retaining frozen CF: {:#}", e);
                    store_clone.retain_frozen_cf(cf_clone.clone());
                    warn!(cf = %cf_clone, retained = store_clone.retained_cf_count(), "Frozen CF retained for recovery");
                }
            }

            Ok(())
        });

        handles.push(handle);
    }

    Ok(handles)
}


/// Stages 1–4: read RocksDB → parse JSON→Arrow → stream Parquet directly to S3.
///
/// Returns a map of table_name → DataFiles for the committer.
/// Uses rt_handle.block_on() for async S3 writes from within the blocking thread.
fn compile_cf_pipeline(
    store: &RocksStore,
    cf_name: &str,
    batch_size: usize,
    batch_bytes: usize,
    parser_threads: usize,
    target_parquet_bytes: usize,
    parquet_row_group_bytes: usize,
    parquet_compression: parquet::basic::Compression,
    table_write_contexts: &HashMap<String, (iceberg::io::FileIO, String, iceberg::spec::SchemaRef)>,
    rt_handle: tokio::runtime::Handle,
) -> Result<HashMap<String, Vec<iceberg::spec::DataFile>>> {
    // Double-buffer only: each queued entry is a full chunk (~flush_compile_batch_bytes
    // of IPC, decoding to ≥ that in Arrow), so depth directly multiplies resident
    // memory per pipeline — 16-deep at 1 parser thread held ~170MB of read-ahead
    // and OOMed the 512Mi pod at conc=2. The writer (S3 upload) is the slowest
    // stage; depth 2 keeps every stage busy without accumulating read-ahead.
    let queue_size = 2;

    // Stage 1 & 2: Reader & Parsers
    let (chunk_tx, chunk_rx) = std::sync::mpsc::sync_channel::<(String, Vec<Vec<u8>>)>(queue_size);
    let (parsed_tx, parsed_rx) = std::sync::mpsc::sync_channel::<(String, Vec<arrow::record_batch::RecordBatch>, usize)>(queue_size);
    let chunk_rx = Arc::new(std::sync::Mutex::new(chunk_rx));

    let mut parser_handles = Vec::new();
    for _ in 0..parser_threads {
        let chunk_rx_clone = Arc::clone(&chunk_rx);
        let parsed_tx_clone = parsed_tx.clone();
        parser_handles.push(tokio::task::spawn_blocking(move || {
            loop {
                let (table_name, chunk) = match chunk_rx_clone.lock().unwrap().recv() {
                    Ok(msg) => msg,
                    Err(_) => break, // Channel closed
                };

                // IPC is self-describing — decode directly, no schema registry lookup needed.
                // Sum raw IPC bytes before consuming chunk — get_array_memory_size over-counts
                // shared buffer capacity in multi-column batches, inflating the threshold ~25×.
                let chunk_bytes: usize = chunk.iter().map(|b| b.len()).sum();
                let mut batches = Vec::new();
                for ipc_bytes in chunk {
                    match crate::arrow_convert::ipc_to_batches(&ipc_bytes) {
                        Ok(bs) => batches.extend(bs),
                        Err(e) => tracing::warn!(table = %table_name, "IPC decode failed: {}", e),
                    }
                }
                if !batches.is_empty() {
                    // Merge the chunk's (mostly single-row) batches so the Parquet
                    // writer sees one batch per chunk instead of thousands.
                    let schema = batches[0].schema();
                    let batches = match arrow::compute::concat_batches(&schema, &batches) {
                        Ok(merged) => vec![merged],
                        Err(e) => {
                            tracing::warn!(table = %table_name, "concat_batches failed, writing unmerged: {}", e);
                            batches
                        }
                    };
                    let _ = parsed_tx_clone.send((table_name, batches, chunk_bytes));
                }
            }
        }));
    }

    let completed_data_files = std::thread::scope(|s| -> Result<HashMap<String, Vec<iceberg::spec::DataFile>>> {
        // Stage 1: Reader (scoped thread).
        // chunk_tx and parsed_tx are moved in so dropping them inside closes the channels.
        s.spawn(move || {
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

                let payload_bytes = &record_bytes[2 + table_name_len..];
                let payload_len = payload_bytes.len();
                let (current_bytes, payload_vec) = raw_batches.entry(table_name.clone()).or_insert((0, Vec::new()));
                payload_vec.push(payload_bytes.to_vec());
                *current_bytes += payload_len;

                if payload_vec.len() >= batch_size || *current_bytes >= batch_bytes {
                    let chunk = std::mem::replace(payload_vec, Vec::with_capacity(batch_size));
                    *current_bytes = 0;
                    let _ = chunk_tx.send((table_name.clone(), chunk));
                }
                Ok(())
            });

            // Drain remaining batches
            for (table_name, (_, payload_vec)) in raw_batches {
                if !payload_vec.is_empty() {
                    let _ = chunk_tx.send((table_name, payload_vec));
                }
            }
            drop(chunk_tx); // Signal parsers to stop
            drop(parsed_tx); // Drop reader's copy; channel closes when parsers also exit
        });

        // Stage 2 drain is handled by the parser_handles (tokio spawn_blocking).
        // Stage 3+4: stream RecordBatches through iceberg's native ParquetWriter stack.
        // RollingFileWriter handles file-size rollover and column stats internally.
        // close() returns Vec<DataFile> with all stats and DataContentType set.
        //
        // table_writers lives outside the streaming closure so that on error we
        // can still close every open writer (finalizing its multipart upload)
        // before propagating the failure and retaining the CF.
        let mut table_writers: HashMap<String, IcebergTableWriter> = HashMap::new();

        let stream_result = (|| -> Result<()> {
            while let Ok((table_name, batches, chunk_bytes)) = parsed_rx.recv() {
                // A missing context means the schema registry was lost (pod
                // restart before the SDK re-registered) or catalog resolution
                // failed this cycle. Either way the records cannot be written;
                // fail the whole CF so it is retained and retried next cycle
                // instead of being silently dropped.
                let (file_io, location, schema_ref) = table_write_contexts
                    .get(&table_name)
                    .with_context(|| format!(
                        "no write context for table '{}' (schema not registered or catalog unavailable)",
                        table_name
                    ))?;

                // Build writer on first batch for this table.
                if !table_writers.contains_key(&table_name) {
                    // parquet only exposes a row-count cap on row groups, so
                    // derive it from the first chunk's average record size to
                    // hit ~parquet_row_group_bytes per group. A completed row
                    // group is the unit that flushes to S3 and frees — this is
                    // the bound on writer memory, independent of file size.
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    let avg_record_bytes = (chunk_bytes / rows.max(1)).max(1);
                    let max_row_group_rows =
                        (parquet_row_group_bytes / avg_record_bytes).clamp(1, 1024 * 1024);
                    let props = parquet::file::properties::WriterProperties::builder()
                        .set_compression(parquet_compression)
                        .set_max_row_group_size(max_row_group_rows)
                        .build();
                    info!(
                        table = %table_name,
                        max_row_group_rows,
                        avg_record_bytes,
                        "Parquet writer opened"
                    );

                    let loc_gen = DefaultLocationGenerator::with_data_location(
                        format!("{}/data", location),
                    );
                    let name_gen = DefaultFileNameGenerator::new(
                        Uuid::new_v4().to_string(),
                        None,
                        DataFileFormat::Parquet,
                    );
                    let parquet_builder = ParquetWriterBuilder::new(
                        props,
                        schema_ref.clone(),
                    );
                    let rolling_builder = RollingFileWriterBuilder::new(
                        parquet_builder,
                        target_parquet_bytes,
                        file_io.clone(),
                        loc_gen,
                        name_gen,
                    );
                    let writer = rt_handle
                        .block_on(DataFileWriterBuilder::new(rolling_builder).build(None))
                        .context("DataFileWriterBuilder::build failed")?;
                    table_writers.insert(table_name.clone(), writer);
                }

                let tw = table_writers.get_mut(&table_name).unwrap();
                for batch in batches {
                    rt_handle
                        .block_on(tw.write(batch))
                        .context("IcebergWriter::write failed")?;
                }
            }
            Ok(())
        })();

        if let Err(e) = stream_result {
            // Best-effort close so multipart uploads are finalized rather than
            // left dangling in S3. The resulting files are never committed; the
            // retained CF re-writes everything on the next cycle.
            for (table_name, mut writer) in table_writers.drain() {
                if let Err(ce) = rt_handle.block_on(writer.close()) {
                    warn!(table = %table_name, "Writer cleanup close failed: {:#}", ce);
                }
            }
            return Err(e);
        }

        // Finalize all writers; each returns DataFiles with column stats already
        // computed. Close every writer even if one fails, then propagate the
        // first error so the CF is retained.
        let mut completed: HashMap<String, Vec<iceberg::spec::DataFile>> = HashMap::new();
        let mut close_err: Option<anyhow::Error> = None;
        for (table_name, mut writer) in table_writers {
            let data_files = match rt_handle.block_on(writer.close()) {
                Ok(dfs) => dfs,
                Err(e) => {
                    error!(table = %table_name, "IcebergWriter::close failed: {:#}", e);
                    close_err.get_or_insert(
                        anyhow::Error::new(e).context("IcebergWriter::close failed"),
                    );
                    continue;
                }
            };
            if !data_files.is_empty() {
                let file_count = data_files.len() as u64;
                let total_bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes()).sum();
                metrics::PARQUET_FILES_WRITTEN.inc_by(file_count);
                metrics::PARQUET_BYTES_WRITTEN.inc_by(total_bytes);
                info!(
                    table = %table_name,
                    files = file_count,
                    bytes = total_bytes,
                    "Parquet files streamed to S3"
                );
                completed.entry(table_name).or_default().extend(data_files);
            }
        }
        if let Some(e) = close_err {
            return Err(e);
        }

        Ok(completed)
    })?;

    Ok(completed_data_files)
}

fn jittered_interval(config: &Config) -> u64 {
    let jitter = rand::thread_rng().gen_range(0..=config.flush_jitter_secs * 2);
    let secs = config
        .flush_interval_secs
        .saturating_add(jitter)
        .saturating_sub(config.flush_jitter_secs);
    secs.max(60)
}
