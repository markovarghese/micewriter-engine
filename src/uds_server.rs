use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::protocol::{
    AckResponse, RegisterSchema, MSG_INGEST_RECORD, MSG_REGISTER_SCHEMA, MSG_FLUSH_NOW,
};
use crate::rocksdb_store::RocksStore;
use crate::config::Config;

pub type SchemaRegistry = Arc<RwLock<HashMap<String, RegisterSchema>>>;

const MAX_PAYLOAD_SIZE: usize = 128 * 1024 * 1024; // 128 MB
const WRITE_BATCH_MAX: usize = 1000;

/// One pending write: the raw payload bytes (including the 1-byte discriminant
/// at offset 0) plus a oneshot the writer task uses to signal persistence.
type WriteRequest = (Vec<u8>, oneshot::Sender<Result<(), String>>);

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

    let (tx, mut rx) = mpsc::channel::<WriteRequest>(100_000);

    let writer_store = Arc::clone(&store);
    let writer_handle = tokio::task::spawn_blocking(move || {
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(WRITE_BATCH_MAX);
        let mut acks: Vec<oneshot::Sender<Result<(), String>>> = Vec::with_capacity(WRITE_BATCH_MAX);

        while let Some((payload, ack)) = rx.blocking_recv() {
            payloads.push(payload);
            acks.push(ack);

            // Opportunistically drain more pending writes into the same batch.
            while payloads.len() < WRITE_BATCH_MAX {
                match rx.try_recv() {
                    Ok((more_payload, more_ack)) => {
                        payloads.push(more_payload);
                        acks.push(more_ack);
                    }
                    Err(_) => break,
                }
            }

            // Strip the 1-byte discriminant from each payload before persisting.
            let bodies: Vec<&[u8]> = payloads.iter().map(|p| &p[1..]).collect();
            let result = writer_store.append_batch(&bodies);

            match result {
                Ok(_) => {
                    for ack in acks.drain(..) {
                        let _ = ack.send(Ok(()));
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    tracing::error!("RocksDB batch append failed: {}", err_str);
                    for ack in acks.drain(..) {
                        let _ = ack.send(Err(err_str.clone()));
                    }
                }
            }
            payloads.clear();
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
    tx: mpsc::Sender<WriteRequest>,
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

/// Parse the header of an MSG_INGEST_RECORD body (everything after the
/// 1-byte discriminant). Returns the table name and the byte offset where
/// the CBOR payload begins. Extracted as a free function so it can be unit
/// tested without spinning up sockets or RocksDB.
fn parse_ingest_header(body: &[u8]) -> Result<(&str, usize), &'static str> {
    if body.len() < 2 {
        return Err("invalid ingest record payload");
    }
    let table_name_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + table_name_len {
        return Err("payload too short for declared table name length");
    }
    let table_name = std::str::from_utf8(&body[2..2 + table_name_len])
        .map_err(|_| "invalid utf-8 in table name")?;
    Ok((table_name, 2 + table_name_len))
}

async fn handle_ingest_record(
    payload: Vec<u8>,
    tx: &mpsc::Sender<WriteRequest>,
    registry: &SchemaRegistry,
) -> AckResponse {
    let body = &payload[1..];

    let (table_name, _cbor_offset) = match parse_ingest_header(body) {
        Ok(v) => v,
        Err(msg) => return AckResponse::error(msg),
    };
    let table_name = table_name.to_string();

    if !registry.read().unwrap().contains_key(&table_name) {
        return AckResponse::error(format!("unknown table '{}' — send REGISTER_SCHEMA first", table_name));
    }

    // Queue the write and wait for the writer task to confirm RocksDB persistence
    // (including fsync, if enabled) before ACKing the SDK.
    let (ack_tx, ack_rx) = oneshot::channel();
    if tx.send((payload, ack_tx)).await.is_err() {
        return AckResponse::error("server shutting down");
    }

    match ack_rx.await {
        Ok(Ok(())) => AckResponse::ok(),
        Ok(Err(e)) => AckResponse::error(format!("rocksdb write failed: {}", e)),
        Err(_) => AckResponse::error("writer task terminated before ack"),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_ingest_header;

    fn make_body(table_name: &[u8], cbor: &[u8]) -> Vec<u8> {
        let len = table_name.len() as u16;
        let mut body = Vec::with_capacity(2 + table_name.len() + cbor.len());
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(table_name);
        body.extend_from_slice(cbor);
        body
    }

    #[test]
    fn parses_valid_header_with_cbor() {
        let body = make_body(b"telemetry_events", &[0xA0]); // empty CBOR map
        let (name, offset) = parse_ingest_header(&body).unwrap();
        assert_eq!(name, "telemetry_events");
        assert_eq!(offset, 2 + b"telemetry_events".len());
        assert_eq!(&body[offset..], &[0xA0]);
    }

    #[test]
    fn parses_minimal_one_byte_cbor() {
        // CBOR null = single byte 0xF6. The old `+ 4` check would have
        // rejected this; the fix must accept it.
        let body = make_body(b"t", &[0xF6]);
        let (name, offset) = parse_ingest_header(&body).unwrap();
        assert_eq!(name, "t");
        assert_eq!(&body[offset..], &[0xF6]);
    }

    #[test]
    fn rejects_body_under_two_bytes() {
        assert_eq!(parse_ingest_header(&[]), Err("invalid ingest record payload"));
        assert_eq!(parse_ingest_header(&[0x00]), Err("invalid ingest record payload"));
    }

    #[test]
    fn rejects_truncated_table_name() {
        // Declares 10-byte name but provides only 3.
        let body = vec![0x00, 0x0A, b'f', b'o', b'o'];
        assert_eq!(
            parse_ingest_header(&body),
            Err("payload too short for declared table name length")
        );
    }

    #[test]
    fn rejects_invalid_utf8_table_name() {
        // 0xFF is not valid UTF-8.
        let body = make_body(&[0xFFu8, 0xFEu8], &[0xA0]);
        assert_eq!(parse_ingest_header(&body), Err("invalid utf-8 in table name"));
    }

    #[test]
    fn accepts_zero_length_table_name_but_loses_no_cbor() {
        // Edge case: u16=0 means table_name is empty. Caller will reject via
        // the schema registry lookup, but the parser itself should succeed.
        let body = make_body(b"", &[0xA0]);
        let (name, offset) = parse_ingest_header(&body).unwrap();
        assert_eq!(name, "");
        assert_eq!(offset, 2);
        assert_eq!(&body[offset..], &[0xA0]);
    }
}
