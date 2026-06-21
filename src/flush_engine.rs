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

type IcebergTableWriter = iceberg::writer::base_writer::data_file_writer::DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

pub async fn run_flush_loop(
    store: Arc<RocksStore>,
    config: Arc<Config>,
    state: Arc<IcebergState>,
    flush_trigger: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    commit_tx: tokio::sync::mpsc::UnboundedSender<CommitRequest>,
    flush_semaphore: Arc<tokio::sync::Semaphore>,
) {
    loop {
        let sleep_secs = if store.retained_cf_count() > 0 {
            10
        } else {
            jittered_interval(&config)
        };
        info!(secs = sleep_secs, retained = store.retained_cf_count(), "Next flush scheduled");

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

        if let Err(e) = do_flush(Arc::clone(&store), &config, Arc::clone(&state), &commit_tx, Arc::clone(&flush_semaphore)).await {
            error!("Flush cycle failed: {:#}", e);
        }
    }
}

pub struct CommitRequest {
    pub cf_name: String,
    pub table_data_files: HashMap<String, Vec<iceberg::spec::DataFile>>,
}

pub async fn run_committer_loop(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<CommitRequest>,
    catalog: Arc<crate::iceberg_writer::CatalogHandle>,
    state: Arc<IcebergState>,
    store: Arc<RocksStore>,
    config: Arc<Config>,
) {
    loop {
        let first_req = match rx.recv().await {
            Some(req) => req,
            None => break,
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

        let mut fully_committed = true;

        for (table_name, data_files) in all_data_files {
            if data_files.is_empty() { continue; }

            let namespace = vec!["analytics".to_string()];

            if let Err(e) = iceberg_writer::commit_data_files(
                &catalog,
                &state,
                &table_name,
                &namespace,
                data_files,
                &config.field_defs,
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

pub async fn do_flush(
    store: Arc<RocksStore>,
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

    let table_name = config.micewriter_table.clone();
    let namespace = vec!["analytics".to_string()];

    // get_table_write_context creates the table if absent and returns (FileIO, location, Iceberg SchemaRef)
    let write_context = iceberg_writer::get_table_write_context(
        &catalog, &state, &table_name, &namespace, &config.field_defs,
    ).await.context("Failed to get table write context")?;

    // Arrow schema for serde_arrow serialization — built from field_defs to avoid
    // any Iceberg→Arrow conversion dependency.
    let arrow_schema = Arc::new(field_defs_to_arrow_schema(&config.field_defs));

    let rt_handle = tokio::runtime::Handle::current();
    let write_context = Arc::new(write_context);

    let mut handles = Vec::new();

    for cf in cfs_to_flush {
        info!(cf = %cf, "Flushing CF in background task");
        let store_clone = Arc::clone(&store);
        let cf_clone = cf.clone();
        let target_parquet_bytes = config.target_parquet_bytes;
        let parquet_row_group_bytes = config.parquet_row_group_bytes;
        let parquet_compression = config.parquet_compression;
        let table_name_clone = table_name.clone();
        let write_context_clone = Arc::clone(&write_context);
        let arrow_schema_clone = Arc::clone(&arrow_schema);
        let rt_handle_clone = rt_handle.clone();

        let commit_tx_clone = commit_tx.clone();
        let sem_clone = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let compile_store = Arc::clone(&store_clone);
            let compile_cf = cf_clone.clone();

            let permit = sem_clone.acquire_owned().await.unwrap();

            let compile_handle = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                compile_cf_pipeline(
                    &compile_store,
                    &compile_cf,
                    target_parquet_bytes,
                    parquet_row_group_bytes,
                    parquet_compression,
                    &write_context_clone,
                    arrow_schema_clone,
                    rt_handle_clone,
                )
            });

            match compile_handle.await.context("compile task panicked")? {
                Ok(data_files) => {
                    let mut table_data_files = HashMap::new();
                    table_data_files.insert(table_name_clone, data_files);
                    let req = CommitRequest {
                        cf_name: cf_clone.clone(),
                        table_data_files,
                    };
                    if let Err(e) = commit_tx_clone.send(req) {
                        error!(cf = %cf_clone, "Failed to send commit request: {}", e);
                        store_clone.retain_frozen_cf(cf_clone);
                    }
                }
                Err(e) => {
                    error!(cf = %cf_clone, "Compile+upload failed, retaining CF: {:#}", e);
                    store_clone.retain_frozen_cf(cf_clone.clone());
                }
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
    target_parquet_bytes: usize,
    parquet_row_group_bytes: usize,
    parquet_compression: parquet::basic::Compression,
    write_context: &(iceberg::io::FileIO, String, iceberg::spec::SchemaRef),
    arrow_schema: std::sync::Arc<arrow::datatypes::Schema>,
    rt_handle: tokio::runtime::Handle,
) -> Result<Vec<iceberg::spec::DataFile>> {
    let (file_io, location, iceberg_schema) = write_context;

    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet_compression)
        .set_max_row_group_size(1024 * 1024)
        .build();

    let loc_gen = DefaultLocationGenerator::with_data_location(format!("{}/data", location));
    let name_gen = DefaultFileNameGenerator::new(Uuid::new_v4().to_string(), None, DataFileFormat::Parquet);
    let parquet_builder = ParquetWriterBuilder::new(props, iceberg_schema.clone());
    let rolling_builder = RollingFileWriterBuilder::new(parquet_builder, target_parquet_bytes, file_io.clone(), loc_gen, name_gen);

    let mut writer = rt_handle
        .block_on(DataFileWriterBuilder::new(rolling_builder).build(None))
        .context("DataFileWriterBuilder::build failed")?;

    let mut events: Vec<crate::schema_codegen::TelemetryEvents> = Vec::new();

    let mut io_err = None;

    // Records in RocksDB are raw CBOR bytes — the [u16 table_name] envelope is
    // stripped at ingest time. The pipeline is pinned to one table, so every
    // record in any CF belongs to that table.
    let _ = store.iterate_cf(cf_name, |cbor_bytes| {
        match ciborium::from_reader::<crate::schema_codegen::TelemetryEvents, _>(cbor_bytes) {
            Ok(event) => events.push(event),
            Err(e) => warn!("Failed to parse CBOR record: {}", e),
        }

        if events.len() >= 10000 {
            if let Err(e) = flush_events_to_writer(&mut writer, &mut events, &arrow_schema, &rt_handle) {
                io_err = Some(e);
                return Err(anyhow::anyhow!("Writer error"));
            }
        }

        Ok(())
    });

    if let Some(e) = io_err {
        let _ = rt_handle.block_on(writer.close());
        return Err(e);
    }

    if !events.is_empty() {
        flush_events_to_writer(&mut writer, &mut events, &arrow_schema, &rt_handle)?;
    }

    let data_files = rt_handle.block_on(writer.close()).context("IcebergWriter::close failed")?;

    if !data_files.is_empty() {
        let file_count = data_files.len() as u64;
        let total_bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes()).sum();
        metrics::PARQUET_FILES_WRITTEN.inc_by(file_count);
        metrics::PARQUET_BYTES_WRITTEN.inc_by(total_bytes);
        info!(files = file_count, bytes = total_bytes, "Parquet files streamed to S3");
    }

    Ok(data_files)
}

fn flush_events_to_writer(
    writer: &mut IcebergTableWriter,
    events: &mut Vec<crate::schema_codegen::TelemetryEvents>,
    arrow_schema: &arrow::datatypes::Schema,
    rt_handle: &tokio::runtime::Handle,
) -> Result<()> {
    let batch = serde_arrow::to_record_batch(&arrow_schema.fields, events)
        .context("Failed to encode struct to Arrow RecordBatch")?;

    rt_handle.block_on(writer.write(batch)).context("IcebergWriter::write failed")?;
    events.clear();
    Ok(())
}

/// Build an Arrow schema from the engine's field definitions.
/// This avoids any Iceberg → Arrow conversion dependency; field types are mapped
/// via the same `MappedType` logic used to build the Iceberg schema.
fn field_defs_to_arrow_schema(
    field_defs: &[crate::field_type::FieldDef],
) -> arrow::datatypes::Schema {
    use crate::field_type::MappedType;
    let mut next_id = 1i32;
    let fields: Vec<arrow::datatypes::Field> = field_defs
        .iter()
        .map(|f| {
            let data_type = MappedType::from_str_or_string(&f.field_type, &f.name)
                .to_arrow(&mut next_id);
            arrow::datatypes::Field::new(&f.name, data_type, !f.required)
        })
        .collect();
    arrow::datatypes::Schema::new(fields)
}

fn jittered_interval(config: &Config) -> u64 {
    let jitter = rand::thread_rng().gen_range(0..=config.flush_jitter_secs * 2);
    let secs = config
        .flush_interval_secs
        .saturating_add(jitter)
        .saturating_sub(config.flush_jitter_secs);
    secs.max(60)
}
