use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::{error, info, warn};

use crate::protocol::{
    AckResponse, RegisterSchema, MSG_INGEST_RECORD, MSG_REGISTER_SCHEMA, MSG_FLUSH_NOW,
};
use crate::rocksdb_store::RocksStore;
use crate::config::Config;

pub type SchemaRegistry = Arc<RwLock<HashMap<String, RegisterSchema>>>;

const MAX_PAYLOAD_SIZE: usize = 128 * 1024 * 1024; // 128 MB

/// Listen on `socket_path` and spawn a handler task for every incoming connection.
///
/// `shutdown` is a one-shot receiver that fires when the engine needs to stop
/// accepting new work (SIGTERM path). In-flight handlers finish naturally.
pub async fn run_server(
    socket_path: &str,
    store: Arc<RocksStore>,
    registry: SchemaRegistry,
    config: Arc<Config>,
    flush_trigger: Arc<tokio::sync::Notify>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    // Remove stale socket file from a previous run.
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)?;
    info!(path = %socket_path, "UDS listener ready");

    let mut join_set = tokio::task::JoinSet::new();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(100_000);
    
    let writer_store = Arc::clone(&store);
    let writer_handle = tokio::task::spawn_blocking(move || {
        while let Some(payload) = rx.blocking_recv() {
            if let Err(e) = writer_store.append(&payload[1..]) {
                tracing::error!("RocksDB append error: {}", e);
            }
            while let Ok(more) = rx.try_recv() {
                if let Err(e) = writer_store.append(&more[1..]) {
                    tracing::error!("RocksDB append error: {}", e);
                }
            }
        }
    });

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let tx_clone = tx.clone();
                        let registry = Arc::clone(&registry);
                        let config = Arc::clone(&config);
                        let flush_trigger = Arc::clone(&flush_trigger);
                        join_set.spawn(async move {
                            if let Err(e) = handle_connection(stream, tx_clone, registry, config, flush_trigger).await {
                                error!("Connection handler error: {:#}", e);
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {}", e),
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("UDS server shutting down - waiting for active connections to drain");
                    break;
                }
            }
        }
    }

    while let Some(res) = join_set.join_next().await {
        if let Err(e) = res {
            error!("Handler panicked: {}", e);
        }
    }

    // Drop the sender so the writer loop will exit once the channel drains
    drop(tx);
    let _ = writer_handle.await;

    Ok(())
}

/// Read IPC frames from one connection until it closes.
async fn handle_connection(
    mut stream: UnixStream,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    registry: SchemaRegistry,
    config: Arc<Config>,
    flush_trigger: Arc<tokio::sync::Notify>,
) -> Result<()> {
    loop {
        // --- Read frame header: 4-byte big-endian total message length ---
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // client closed
            Err(e) => return Err(e.into()),
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len == 0 {
            continue;
        }

        if msg_len > MAX_PAYLOAD_SIZE {
            error!("Payload size {} exceeds maximum allowed ({}). Closing connection.", msg_len, MAX_PAYLOAD_SIZE);
            return Err(anyhow::anyhow!("payload too large"));
        }

        // --- Read the full payload ---
        let mut payload = vec![0u8; msg_len];
        stream.read_exact(&mut payload).await?;

        // --- First byte = message type discriminant ---
        let msg_type = payload[0];

        let ack = match msg_type {
            MSG_REGISTER_SCHEMA => handle_register_schema(&payload[1..], &registry),
            MSG_INGEST_RECORD => handle_ingest_record(payload, &tx, &registry).await,
            MSG_FLUSH_NOW => handle_flush_now(&config, &flush_trigger),
            other => {
                warn!(byte = other, "Unknown message type");
                AckResponse::error(format!("unknown message type 0x{:02X}", other))
            }
        };

        // --- Send ACK: 4-byte length prefix + JSON body ---
        let ack_bytes = serde_json::to_vec(&ack)?;
        let mut out = BytesMut::with_capacity(4 + ack_bytes.len());
        out.put_u32(ack_bytes.len() as u32);
        out.extend_from_slice(&ack_bytes);
        stream.write_all(&out).await?;
    }

    Ok(())
}

fn handle_register_schema(body: &[u8], registry: &SchemaRegistry) -> AckResponse {
    match serde_json::from_slice::<RegisterSchema>(body) {
        Ok(schema) => {
            let table = schema.table.clone();
            registry.write().unwrap().insert(table.clone(), schema);
            info!(table = %table, "Schema registered");
            AckResponse::ok()
        }
        Err(e) => {
            error!("Failed to deserialize RegisterSchema: {}", e);
            AckResponse::error(e.to_string())
        }
    }
}

fn handle_flush_now(config: &Config, flush_trigger: &tokio::sync::Notify) -> AckResponse {
    if config.enable_manual_flush {
        flush_trigger.notify_one();
        info!("Manual flush requested by client");
        AckResponse::ok()
    } else {
        warn!("Client requested manual flush, but ENABLE_MANUAL_FLUSH is false");
        AckResponse::error("manual flush is disabled on this server")
    }
}

async fn handle_ingest_record(
    payload: Vec<u8>,
    tx: &tokio::sync::mpsc::Sender<Vec<u8>>,
    registry: &SchemaRegistry,
) -> AckResponse {
    let body = &payload[1..];

    if body.len() < 2 {
        return AckResponse::error("invalid ingest record payload");
    }
    
    let table_name_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + table_name_len + 4 {
        return AckResponse::error("payload too short for table name and schema id");
    }
    
    let table_name_bytes = &body[2..2 + table_name_len];
    let table_name = match std::str::from_utf8(table_name_bytes) {
        Ok(s) => s.to_string(),
        Err(_) => return AckResponse::error("invalid utf-8 in table name"),
    };

    if !registry.read().unwrap().contains_key(&table_name) {
        return AckResponse::error(format!("unknown table '{}' — send REGISTER_SCHEMA first", table_name));
    }

    match tx.send(payload).await {
        Ok(_) => AckResponse::ok(),
        Err(_) => AckResponse::error("server shutting down"),
    }
}
