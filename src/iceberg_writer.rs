use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogBuilder};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{CatalogType, Config};
use crate::protocol::FieldDef;

/// Process-lifetime caches to avoid redundant Glue/Nessie metadata API calls.
///
/// Namespaces and tables are never deleted by this service, so once confirmed
/// to exist they remain valid for the lifetime of the process. On pod restart
/// the caches are cold and the first cycle re-validates.
#[derive(Clone, Default)]
pub struct IcebergState {
    /// Namespaces confirmed to exist. Key: namespace segments joined with "/".
    pub known_namespaces: Arc<RwLock<HashSet<String>>>,
    /// Tables confirmed to exist. Key: "namespace/table_name".
    pub known_tables: Arc<RwLock<HashSet<String>>>,
}

/// Catalog handle that avoids `Box<dyn Catalog>` (the trait is not object-safe).
pub enum CatalogHandle {
    Glue(GlueCatalog),
    Nessie(RestCatalog),
}

/// Build the configured catalog. Call once per flush cycle, not once per table.
pub async fn build_catalog(config: &Config) -> Result<CatalogHandle> {
    match config.catalog_type {
        CatalogType::Glue => build_glue_catalog(config).await.map(CatalogHandle::Glue),
        CatalogType::Nessie => build_nessie_catalog(config).await.map(CatalogHandle::Nessie),
    }
}

/// Flush one table's Parquet bytes to S3 and commit to the configured catalog.
pub async fn flush_table(
    catalog: &CatalogHandle,
    state: &IcebergState,
    table_name: &str,
    namespace: &[String],
    parquet_bytes: Vec<u8>,
    field_defs: &[FieldDef],
    config: &Config,
) -> Result<()> {
    if parquet_bytes.is_empty() {
        return Ok(());
    }
    match catalog {
        CatalogHandle::Glue(c) => {
            do_flush_table(c, state, table_name, namespace, parquet_bytes, field_defs, config).await
        }
        CatalogHandle::Nessie(c) => {
            do_flush_table(c, state, table_name, namespace, parquet_bytes, field_defs, config).await
        }
    }
}

async fn do_flush_table<C: Catalog>(
    catalog: &C,
    state: &IcebergState,
    table_name: &str,
    namespace: &[String],
    parquet_bytes: Vec<u8>,
    field_defs: &[FieldDef],
    _config: &Config,
) -> Result<()> {
    let ns_ident = NamespaceIdent::from_vec(namespace.to_vec())?;
    let table_ident = TableIdent::new(ns_ident.clone(), table_name.to_string());

    // Ensure namespace exists. Skip the catalog call after the first confirmation —
    // namespaces are never deleted by this service so the cached result stays valid.
    let ns_key = namespace.join("/");
    if !state.known_namespaces.read().unwrap().contains(&ns_key) {
        if !catalog.namespace_exists(&ns_ident).await? {
            catalog.create_namespace(&ns_ident, HashMap::new()).await?;
            info!(namespace = ?namespace, "Created Iceberg namespace");
        }
        state.known_namespaces.write().unwrap().insert(ns_key);
    }

    // Load or create the Iceberg table.
    // On the first encounter for this process lifetime use the standard probe.
    // Once cached, skip the existence check and go straight to load_table —
    // the load is still required to obtain fresh snapshot metadata for the commit.
    let table_key = format!("{}/{}", namespace.join("/"), table_name);
    let table_known = state.known_tables.read().unwrap().contains(&table_key);

    let table = if table_known {
        catalog.load_table(&table_ident).await?
    } else {
        let t = if catalog.table_exists(&table_ident).await? {
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
        state.known_tables.write().unwrap().insert(table_key);
        t
    };

    // Derive the S3 path for this data file.
    let file_path = format!(
        "{}/data/{}.parquet",
        table.metadata().location(),
        Uuid::new_v4()
    );

    // Write Parquet bytes to S3 via the table's FileIO abstraction.
    let file_io = table.file_io();
    let output = file_io.new_output(&file_path)?;
    let mut writer = output.writer().await?;
    writer.write(parquet_bytes.clone().into()).await?;
    writer.close().await?;

    info!(path = %file_path, bytes = parquet_bytes.len(), "Parquet file uploaded to S3");

    // Build the DataFile descriptor.
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .record_count(0)
        .file_size_in_bytes(parquet_bytes.len() as u64)
        .build()?;

    // Commit with exponential backoff on optimistic locking conflicts.
    commit_with_retry(catalog, &table, data_file).await?;

    info!(table = %table_name, "Iceberg commit successful");
    Ok(())
}

async fn commit_with_retry<C: Catalog>(
    catalog: &C,
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

        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        let result = async {
            let tx = Transaction::new(&fresh_table);
            let action = tx.fast_append().add_data_files(vec![data_file.clone()]);
            let tx = action.apply(tx)?;
            tx.commit(catalog).await
        }
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

async fn build_nessie_catalog(config: &Config) -> Result<RestCatalog> {
    let mut props = HashMap::from([
        ("s3.endpoint".to_string(), config.minio_url.clone().unwrap()),
        (
            "s3.access-key-id".to_string(),
            config.minio_access_key.clone().unwrap(),
        ),
        (
            "s3.secret-access-key".to_string(),
            config.minio_secret_key.clone().unwrap(),
        ),
        ("s3.path-style-access".to_string(), "true".to_string()),
        ("warehouse".to_string(), config.warehouse.clone()),
    ]);
    props.insert("uri".to_string(), config.nessie_uri.clone().unwrap());

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(iceberg_storage_opendal::OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .load("rest_catalog", props)
        .await?;

    Ok(catalog)
}

async fn build_glue_catalog(config: &Config) -> Result<GlueCatalog> {
    let mut props = HashMap::new();
    props.insert("warehouse".to_string(), config.warehouse.clone());
    if let Some(ref catalog_id) = config.glue_catalog_id {
        props.insert("catalog_id".to_string(), catalog_id.clone());
    }

    let catalog = GlueCatalogBuilder::default()
        .load("glue_catalog", props)
        .await?;

    Ok(catalog)
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
