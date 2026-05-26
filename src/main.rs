mod config;
mod flush_engine;
mod iceberg_writer;
mod parquet_writer;
mod protocol;
mod rocksdb_store;
mod uds_server;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("micewriter_engine=info".parse()?),
        )
        .init();

    let config = Arc::new(config::Config::from_env()?);
    info!("mIceWriter Engine starting");

    let store = Arc::new(rocksdb_store::RocksStore::open(&config.rocksdb_path)?);
    let registry: uds_server::SchemaRegistry = Arc::new(RwLock::new(HashMap::new()));
    let iceberg_state = Arc::new(iceberg_writer::IcebergState::default());

    // Channel used to signal the UDS server to stop accepting new connections.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn the UDS server.
    let uds_store = Arc::clone(&store);
    let uds_registry = Arc::clone(&registry);
    let uds_socket = config.socket_path.clone();
    let uds_handle = tokio::spawn(async move {
        if let Err(e) =
            uds_server::run_server(&uds_socket, uds_store, uds_registry, shutdown_rx).await
        {
            tracing::error!("UDS server error: {:#}", e);
        }
    });

    // Spawn the background flush loop.
    let flush_store = Arc::clone(&store);
    let flush_registry = Arc::clone(&registry);
    let flush_config = Arc::clone(&config);
    let flush_state = Arc::clone(&iceberg_state);
    tokio::spawn(async move {
        flush_engine::run_flush_loop(flush_store, flush_registry, flush_config, flush_state).await;
    });

    // Wait for SIGTERM (Kubernetes pod termination) or Ctrl+C (local dev).
    wait_for_shutdown().await;

    info!("Shutdown signal received — stopping UDS server and flushing remaining data");

    // Stop the UDS server from accepting new connections.
    let _ = shutdown_tx.send(true);

    info!("Waiting for active UDS connections to drain...");
    let _ = uds_handle.await;

    // Emergency flush: drain anything in the active CF before exiting.
    if let Err(e) =
        flush_engine::do_flush(Arc::clone(&store), &registry, &config, &iceberg_state).await
    {
        tracing::error!("Emergency flush failed: {:#}", e);
    } else {
        info!("Emergency flush complete");
    }

    info!("mIceWriter Engine exited cleanly");
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = sigterm.recv() => { info!("Received SIGTERM"); }
        _ = tokio::signal::ctrl_c() => { info!("Received Ctrl+C"); }
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl+C");
    info!("Received Ctrl+C");
}
