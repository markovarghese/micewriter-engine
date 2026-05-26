use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::{error, info, warn};

use crate::protocol::{
    AckResponse, FieldDef, IngestRecord, RegisterSchema, MSG_INGEST_RECORD, MSG_REGISTER_SCHEMA,
};
use crate::rocksdb_store::RocksStore;

pub type SchemaRegistry = Arc<RwLock<HashMap<String, RegisterSchema>>>;

/// Listen on `socket_path` and spawn a handler task for every incoming connection.
///
/// `shutdown` is a one-shot receiver that fires when the engine needs to stop
/// accepting new work (SIGTERM path). In-flight handlers finish naturally.
pub async fn run_server(
    socket_path: &str,
    store: Arc<RocksStore>,
    registry: SchemaRegistry,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    // Remove stale socket file from a previous run.
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)?;
    info!(path = %socket_path, "UDS listener ready");

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let store = Arc::clone(&store);
                        let registry = Arc::clone(&registry);
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, store, registry).await {
                                error!("Connection handler error: {:#}", e);
                            }
                        });
                    }
                    Err(e) => error!("Accept error: {}", e),
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("UDS server shutting down");
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Read IPC frames from one connection until it closes.
async fn handle_connection(
    mut stream: UnixStream,
    store: Arc<RocksStore>,
    registry: SchemaRegistry,
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

        // --- Read the full payload ---
        let mut payload = vec![0u8; msg_len];
        stream.read_exact(&mut payload).await?;

        // --- First byte = message type discriminant ---
        let msg_type = payload[0];
        let body = &payload[1..];

        let ack = match msg_type {
            MSG_REGISTER_SCHEMA => handle_register_schema(body, &registry),
            MSG_INGEST_RECORD => handle_ingest_record(body, &store, &registry),
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

fn handle_ingest_record(
    body: &[u8],
    store: &RocksStore,
    registry: &SchemaRegistry,
) -> AckResponse {
    // Validate the table is known before writing to RocksDB.
    let record: IngestRecord = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to deserialize IngestRecord: {}", e);
            return AckResponse::error(e.to_string());
        }
    };

    if !registry.read().unwrap().contains_key(&record.table) {
        return AckResponse::error(format!("unknown table '{}' — send REGISTER_SCHEMA first", record.table));
    }

    // Store the raw JSON bytes so the flush engine can reconstruct field values.
    match store.append(body) {
        Ok(_) => AckResponse::ok(),
        Err(e) => {
            error!("RocksDB append error: {}", e);
            AckResponse::error(e.to_string())
        }
    }
}
