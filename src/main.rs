mod config;
mod field_type;
mod flush_engine;
mod iceberg_writer;
mod metrics;
mod protocol;
mod rocksdb_store;
mod uds_server;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // pprof needs a writable /tmp, but rootfs is read-only.
    std::env::set_var("TMPDIR", "/var/run/app");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("micewriter_engine=info".parse()?),
        )
        .init();

    let config = Arc::new(config::Config::from_env()?);


    info!("mIceWriter Engine starting");

    let store = Arc::new(rocksdb_store::RocksStore::new(
        &config.rocksdb_path,
        config.flush_size_bytes,
        config.flush_size_jitter_bytes,
        config.rocksdb_sync_writes,
        config.write_buffer_size,
    )?);
    let registry: uds_server::SchemaRegistry = Arc::new(RwLock::new(HashMap::new()));
    let iceberg_state = Arc::new(iceberg_writer::IcebergState::default());

    let flush_trigger = Arc::new(tokio::sync::Notify::new());

    // Channel used to signal the UDS server and flush loop to stop.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Register Prometheus metrics
    metrics::register_custom_metrics();

    // Spawn the debug HTTP server
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/debug/pprof/flamegraph",
            axum::routing::get(|| async {
                use axum::response::IntoResponse;
                let guard = pprof::ProfilerGuardBuilder::default()
                    .frequency(100)
                    .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                    .build()
                    .unwrap();

                // Profile for 15 seconds
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;

                if let Ok(report) = guard.report().build() {
                    let mut body = Vec::new();
                    report.flamegraph(&mut body).unwrap();
                    (
                        [(axum::http::header::CONTENT_TYPE, "image/svg+xml")],
                        body,
                    )
                } else {
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/plain")],
                        b"Failed to build flamegraph".to_vec(),
                    )
                }
            }),
        )
        .route(
            "/metrics",
            axum::routing::get(|| async {
                (
                    [(axum::http::header::CONTENT_TYPE, "text/plain")],
                    metrics::gather_metrics(),
                )
            }),
        );

        let listener = tokio::net::TcpListener::bind("0.0.0.0:8088").await.unwrap();
        tracing::info!("Debug HTTP server listening on 0.0.0.0:8088");
        axum::serve(listener, app).await.unwrap();
    });

    // Spawn the UDS server.
    let uds_store = Arc::clone(&store);
    let uds_registry = Arc::clone(&registry);
    let uds_config = Arc::clone(&config);
    let uds_flush_trigger = Arc::clone(&flush_trigger);
    let uds_socket = config.socket_path.clone();
    let uds_shutdown_rx = shutdown_rx.clone();
    let uds_handle = tokio::spawn(async move {
        if let Err(e) =
            uds_server::run_server(&uds_socket, uds_store, uds_registry, uds_config, uds_flush_trigger, uds_shutdown_rx).await
        {
            tracing::error!("UDS server error: {:#}", e);
        }
    });

    let (commit_tx, commit_rx) = tokio::sync::mpsc::unbounded_channel::<flush_engine::CommitRequest>();

    let committer_catalog = match iceberg_writer::build_catalog(&config).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to build Iceberg catalog for committer: {:#}", e);
            return Err(e);
        }
    };
    let committer_state = Arc::clone(&iceberg_state);
    let committer_registry = Arc::clone(&registry);
    let committer_store = Arc::clone(&store);

    let committer_handle = tokio::spawn(async move {
        flush_engine::run_committer_loop(
            commit_rx,
            committer_catalog,
            committer_state,
            committer_registry,
            committer_store
        ).await;
    });

    // Spawn the background flush loop.
    let flush_store = Arc::clone(&store);
    let flush_registry = Arc::clone(&registry);
    let flush_config = Arc::clone(&config);
    let flush_state = Arc::clone(&iceberg_state);
    let flush_loop_trigger = Arc::clone(&flush_trigger);
    let flush_shutdown_rx = shutdown_rx.clone();
    let flush_commit_tx = commit_tx.clone();
    let flush_loop_handle = tokio::spawn(async move {
        flush_engine::run_flush_loop(
            flush_store,
            flush_registry,
            flush_config,
            flush_state,
            flush_loop_trigger,
            flush_shutdown_rx,
            flush_commit_tx,
        ).await;
    });

    // Wait for SIGTERM (Kubernetes pod termination) or Ctrl+C (local dev).
    wait_for_shutdown().await;

    info!("Shutdown signal received — stopping UDS server and flushing remaining data");

    // Tell the UDS server and the background flush loop to stop.
    let _ = shutdown_tx.send(true);

    info!("Waiting for active UDS connections to drain...");
    let _ = uds_handle.await;

    info!("Waiting for background flush loop to finish...");
    let _ = flush_loop_handle.await;

    // Emergency flush: drain anything in the active CF before exiting.
    match flush_engine::do_flush(Arc::clone(&store), &registry, &config, Arc::clone(&iceberg_state), &commit_tx).await {
        Ok(handles) => {
            info!("Emergency flush spawned, waiting for Parquet compilation & S3 uploads...");
            for handle in handles {
                let _ = handle.await;
            }
            info!("Emergency flush complete");
        }
        Err(e) => tracing::error!("Emergency flush failed: {:#}", e),
    }

    info!("Waiting for Iceberg committer loop to drain its queue and finish...");
    drop(commit_tx);
    let _ = committer_handle.await;

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
