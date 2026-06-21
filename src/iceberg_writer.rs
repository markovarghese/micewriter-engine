use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use iceberg::spec::{NestedField, Schema, SchemaRef};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogBuilder};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use tracing::{info, warn};

use crate::config::{CatalogType, Config};
use crate::field_type::MappedType;
use crate::metrics;
use crate::field_type::FieldDef;

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
pub async fn build_catalog(config: &Config) -> Result<Arc<CatalogHandle>> {
    let handle = match config.catalog_type {
        CatalogType::Glue => build_glue_catalog(config).await.map(CatalogHandle::Glue)?,
        CatalogType::Nessie => build_nessie_catalog(config).await.map(CatalogHandle::Nessie)?,
    };
    Ok(Arc::new(handle))
}

/// Pre-resolve a table's FileIO, warehouse location, and Iceberg schema for streaming writes.
/// Creates the table/namespace if not yet present. Called once per flush cycle
/// before entering the blocking compile phase.
pub async fn get_table_write_context(
    catalog: &CatalogHandle,
    state: &IcebergState,
    table_name: &str,
    namespace: &[String],
    field_defs: &[FieldDef],
) -> Result<(iceberg::io::FileIO, String, SchemaRef)> {
    match catalog {
        CatalogHandle::Glue(c) => {
            let table = get_or_create_table(c, state, table_name, namespace, field_defs).await?;
            Ok((
                table.file_io().clone(),
                table.metadata().location().to_string(),
                table.metadata().current_schema().clone(),
            ))
        }
        CatalogHandle::Nessie(c) => {
            let table = get_or_create_table(c, state, table_name, namespace, field_defs).await?;
            Ok((
                table.file_io().clone(),
                table.metadata().location().to_string(),
                table.metadata().current_schema().clone(),
            ))
        }
    }
}

pub async fn commit_data_files(
    catalog: &CatalogHandle,
    state: &IcebergState,
    table_name: &str,
    namespace: &[String],
    data_files: Vec<iceberg::spec::DataFile>,
    field_defs: &[FieldDef],
) -> Result<()> {
    if data_files.is_empty() {
        return Ok(());
    }
    match catalog {
        CatalogHandle::Glue(c) => {
            do_commit_data_files(c, state, table_name, namespace, data_files, field_defs).await
        }
        CatalogHandle::Nessie(c) => {
            do_commit_data_files(c, state, table_name, namespace, data_files, field_defs).await
        }
    }
}

async fn get_or_create_table<C: Catalog>(
    catalog: &C,
    state: &IcebergState,
    table_name: &str,
    namespace: &[String],
    field_defs: &[FieldDef],
) -> Result<iceberg::table::Table> {
    let ns_ident = NamespaceIdent::from_vec(namespace.to_vec())?;
    let table_ident = TableIdent::new(ns_ident.clone(), table_name.to_string());

    let ns_key = namespace.join("/");
    if !state.known_namespaces.read().unwrap().contains(&ns_key) {
        if !catalog.namespace_exists(&ns_ident).await? {
            catalog.create_namespace(&ns_ident, HashMap::new()).await?;
            info!(namespace = ?namespace, "Created Iceberg namespace");
        }
        state.known_namespaces.write().unwrap().insert(ns_key);
    }

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
    Ok(table)
}

async fn do_commit_data_files<C: Catalog>(
    catalog: &C,
    state: &IcebergState,
    table_name: &str,
    namespace: &[String],
    data_files: Vec<iceberg::spec::DataFile>,
    field_defs: &[FieldDef],
) -> Result<()> {
    let table = get_or_create_table(catalog, state, table_name, namespace, field_defs).await?;
    commit_with_retry(catalog, &table, data_files).await?;
    info!(table = %table_name, "Iceberg commit successful");

    metrics::CATALOG_COMMITS.inc();
    Ok(())
}

async fn commit_with_retry<C: Catalog>(
    catalog: &C,
    table: &iceberg::table::Table,
    data_files: Vec<iceberg::spec::DataFile>,
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
            let action = tx.fast_append().add_data_files(data_files.clone());
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
    let mut next_id = 1;
    let nested: Vec<_> = fields
        .iter()
        .map(|f| {
            let field_id = next_id;
            next_id += 1;
            let field_type = MappedType::from_str_or_string(&f.field_type, &f.name).to_iceberg(&mut next_id);
            if f.required {
                NestedField::required(field_id, &f.name, field_type)
            } else {
                NestedField::optional(field_id, &f.name, field_type)
            }
        })
        .map(Arc::new)
        .collect();

    Schema::builder()
        .with_fields(nested)
        .build()
        .context("Failed to build Iceberg schema")
}
