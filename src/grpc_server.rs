pub mod micewriter_pb {
    tonic::include_proto!("micewriter.v2");
}

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::config::Config;
use crate::rocksdb_store::RocksStore;
use micewriter_pb::micewriter_server::{Micewriter, MicewriterServer};
use micewriter_pb::{Ack, FlushRequest, IngestRecord, RegisterSchemaRequest};

pub struct MicewriterService {
    pub store: Arc<RocksStore>,
    pub config: Arc<Config>,
    pub flush_trigger: Arc<tokio::sync::Notify>,
}

#[tonic::async_trait]
impl Micewriter for MicewriterService {
    type IngestStream = ReceiverStream<Result<Ack, Status>>;

    async fn register_schema(
        &self,
        request: Request<RegisterSchemaRequest>,
    ) -> Result<Response<Ack>, Status> {
        let _schema_json = request.into_inner().schema_json;
        // In the AOT architecture, the schema is statically compiled. 
        // We could validate that the incoming schema_json matches the expected static schema, 
        // but for now we just acknowledge it gracefully to unblock the SDK.
        Ok(Response::new(Ack {
            ok: true,
            message: "Schema acknowledged (AOT static schema is active)".to_string(),
        }))
    }

    async fn ingest(
        &self,
        request: Request<Streaming<IngestRecord>>,
    ) -> Result<Response<Self::IngestStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        
        let expected_table = self.config.micewriter_table.clone();
        let store = Arc::clone(&self.store);
        let flush_trigger = Arc::clone(&self.flush_trigger);

        tokio::spawn(async move {
            while let Ok(Some(msg)) = stream.message().await {
                let payload = msg.record;
                if payload.len() < 2 {
                    let _ = tx.send(Ok(Ack { ok: false, message: "Payload too small".to_string() })).await;
                    continue;
                }

                let table_name_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                if payload.len() < 2 + table_name_len {
                    let _ = tx.send(Ok(Ack { ok: false, message: "Invalid payload formatting".to_string() })).await;
                    continue;
                }

                let table_name = String::from_utf8_lossy(&payload[2..2 + table_name_len]);
                if table_name != expected_table {
                    let _ = tx.send(Ok(Ack {
                        ok: false,
                        message: format!("Cross-table write rejected. Engine is pinned to {}", expected_table)
                    })).await;
                    continue;
                }

                let cbor_bytes = &payload[2 + table_name_len..];

                match store.append_batch(&[cbor_bytes]) {
                    Ok(size_exceeded) => {
                        if size_exceeded {
                            flush_trigger.notify_one();
                        }
                        if tx.send(Ok(Ack { ok: true, message: "OK".to_string() })).await.is_err() {
                            break; // Client disconnected
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to append record to RocksDB: {}", e);
                        let _ = tx.send(Ok(Ack { ok: false, message: "Internal storage error".to_string() })).await;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn flush_now(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<Ack>, Status> {
        if !self.config.enable_manual_flush {
            return Ok(Response::new(Ack {
                ok: false,
                message: "Manual flush is disabled in production".to_string(),
            }));
        }

        self.flush_trigger.notify_one();
        Ok(Response::new(Ack {
            ok: true,
            message: "Flush triggered".to_string(),
        }))
    }
}

pub async fn run_server(
    store: Arc<RocksStore>,
    config: Arc<Config>,
    flush_trigger: Arc<tokio::sync::Notify>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", config.grpc_port).parse()?;
    
    let service = MicewriterService {
        store,
        config: Arc::clone(&config),
        flush_trigger,
    };

    tracing::info!("gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(MicewriterServer::new(service))
        .serve_with_shutdown(addr, async move {
            let _ = shutdown_rx.changed().await;
            tracing::info!("gRPC server shutting down");
        })
        .await?;

    Ok(())
}
