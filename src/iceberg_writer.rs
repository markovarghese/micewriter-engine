use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::protocol::FieldDef;

/// Flush one table's Parquet bytes to MinIO and commit to the Nessie catalog.
///
/// Retries the catalog commit up to `max_attempts` times with exponential backoff
/// to handle optimistic locking conflicts (`CommitFailedException`).
pub async fn flush_table(
    table_name: &str,
    namespace: &[String],
    parquet_bytes: Vec<u8>,
    field_defs: &[FieldDef],
    config: &Config,
) -> Result<()> {
    if parquet_bytes.is_empty() {
        return Ok(());
    }

    let catalog = build_catalog(config).await?;

    let ns_ident = NamespaceIdent::from_vec(namespace.to_vec())?;
    let table_ident = TableIdent::new(ns_ident.clone(), table_name.to_string());

    // Ensure namespace exists.
    if !catalog.namespace_exists(&ns_ident).await? {
        catalog.create_namespace(&ns_ident, HashMap::new()).await?;
        info!(namespace = ?namespace, "Created Iceberg namespace");
    }

    // Load or create the Iceberg table.
    let table = if catalog.table_exists(&table_ident).await? {
        catalog.load_table(&table_ident).await?
    } else {
        let schema = build_iceberg_schema(field_defs)?;
        let creation = TableCreation::builder()
            .name(table_name.to_string())
            .schema(schema)
            .build();
        let t = catalog.create_table(&ns_ident, creation).await?;
        info!(table = %table_name, "Created Iceberg table");
        t
    };

    // Derive the S3 path for this data file.
    let file_path = format!(
        "{}/data/{}.parquet",
        table.metadata().location(),
        Uuid::new_v4()
    );

    // Write Parquet bytes to MinIO via the table's FileIO abstraction.
    let file_io = table.file_io();
    let output = file_io.new_output(&file_path)?;
    let mut writer = output.writer().await?;
    writer.write_all(&parquet_bytes).await?;
    writer.close().await?;

    info!(path = %file_path, bytes = parquet_bytes.len(), "Parquet file uploaded to MinIO");

    // Build the DataFile descriptor.
    use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, FileFormat};
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(FileFormat::Parquet)
        .record_count(0) // Iceberg allows 0 here; real count improves stats
        .file_size_in_bytes(parquet_bytes.len() as i64)
        .build()?;

    // Commit with exponential backoff on optimistic locking conflicts.
    commit_with_retry(&catalog, &table, data_file).await?;

    info!(table = %table_name, "Iceberg commit successful");
    Ok(())
}

async fn commit_with_retry(
    catalog: &RestCatalog,
    table: &iceberg::table::Table,
    data_file: iceberg::spec::DataFile,
) -> Result<()> {
    let max_attempts = 5u32;
    let mut delay = Duration::from_millis(200);

    for attempt in 1..=max_attempts {
        // Reload the table on every retry to get the latest snapshot.
        let fresh_table = if attempt > 1 {
            catalog.load_table(table.identifier()).await?
        } else {
            table.clone()
        };

        use iceberg::transaction::Transaction;
        let tx = Transaction::new(&fresh_table);
        let result = tx
            .fast_append(None, vec![data_file.clone()])?
            .commit(catalog)
            .await;

        match result {
            Ok(_) => return Ok(()),
            Err(e) if attempt < max_attempts => {
                warn!(attempt, error = %e, delay_ms = delay.as_millis(), "Commit failed, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
            Err(e) => return Err(e).context("Iceberg commit failed after all retries"),
        }
    }

    unreachable!()
}

async fn build_catalog(config: &Config) -> Result<RestCatalog> {
    let catalog_config = RestCatalogConfig::builder()
        .uri(config.nessie_uri.clone())
        .warehouse(config.nessie_warehouse.clone())
        .props([
            ("s3.endpoint".to_string(), config.minio_url.clone()),
            ("s3.access-key-id".to_string(), config.minio_access_key.clone()),
            ("s3.secret-access-key".to_string(), config.minio_secret_key.clone()),
            // MinIO requires path-style S3 addressing.
            ("s3.path-style-access".to_string(), "true".to_string()),
        ])
        .build();

    Ok(RestCatalog::new(catalog_config))
}

fn build_iceberg_schema(fields: &[FieldDef]) -> Result<Schema> {
    let nested: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let field_type = map_primitive_type(&f.field_type);
            if f.required {
                NestedField::required(i as i32 + 1, &f.name, Type::Primitive(field_type))
            } else {
                NestedField::optional(i as i32 + 1, &f.name, Type::Primitive(field_type))
            }
        })
        .map(Arc::new)
        .collect();

    Schema::builder()
        .with_fields(nested)
        .build()
        .context("Failed to build Iceberg schema")
}

fn map_primitive_type(type_str: &str) -> PrimitiveType {
    match type_str {
        "string" => PrimitiveType::String,
        "long" | "int64" => PrimitiveType::Long,
        "int" | "int32" => PrimitiveType::Int,
        "double" | "float64" => PrimitiveType::Double,
        "float" | "float32" => PrimitiveType::Float,
        "boolean" => PrimitiveType::Boolean,
        "timestamptz" => PrimitiveType::Timestamptz,
        "timestamp" => PrimitiveType::Timestamp,
        "date" => PrimitiveType::Date,
        "binary" | "bytes" => PrimitiveType::Binary,
        _ => PrimitiveType::String,
    }
}
