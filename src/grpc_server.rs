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
        let schema_json = request.into_inner().schema_json;
        match validate_register_schema(&schema_json, &self.config.micewriter_table, &self.config.field_defs) {
            Ok(field_count) => {
                tracing::info!(
                    table = %self.config.micewriter_table,
                    fields = field_count,
                    "RegisterSchema: validated successfully"
                );
                Ok(Response::new(Ack {
                    ok: true,
                    message: format!("Schema validated — {} fields match AOT schema", field_count),
                }))
            }
            Err(msg) => {
                tracing::error!(
                    table = %self.config.micewriter_table,
                    "RegisterSchema rejected: {}",
                    msg
                );
                Ok(Response::new(Ack { ok: false, message: msg }))
            }
        }
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

/// Validate an incoming `RegisterSchema` JSON payload against the engine's AOT field definitions.
///
/// Returns `Ok(field_count)` on success or `Err(human-readable message)` on any violation.
/// Extracted as a pure function so it can be unit-tested without a live gRPC service.
///
/// Rules (from system-overview.md §2.3):
/// - Every field the SDK declares must exist in the AOT schema (unknown field → misordered deploy).
/// - Every field's type must match the AOT type (alias-normalised via MappedType).
/// - `table` in the payload must match the engine's pinned table name.
/// - AOT fields absent from the incoming schema are allowed (engine deployed first — valid evolution).
fn validate_register_schema(
    schema_json: &str,
    engine_table: &str,
    field_defs: &[crate::field_type::FieldDef],
) -> Result<usize, String> {
    use crate::field_type::MappedType;

    let incoming: serde_json::Value = serde_json::from_str(schema_json)
        .map_err(|e| format!("Invalid schema JSON: {e}"))?;

    if let Some(table) = incoming.get("table").and_then(|t| t.as_str()) {
        if table != engine_table {
            return Err(format!(
                "Table mismatch: engine is pinned to '{engine_table}', schema is for '{table}'"
            ));
        }
    }

    let fields = incoming
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| "Schema JSON missing 'fields' array".to_string())?;

    for field in fields {
        let name = field.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let type_str = field.get("type").and_then(|t| t.as_str()).unwrap_or("");

        let aot = field_defs.iter().find(|f| f.name == name).ok_or_else(|| {
            format!(
                "Unknown field '{name}' in table '{engine_table}': not present in AOT schema. \
                 The engine must be deployed and stabilized before the application."
            )
        })?;

        let incoming_mapped = MappedType::from_str(type_str);
        let aot_mapped = MappedType::from_str(&aot.field_type);
        if incoming_mapped != aot_mapped {
            return Err(format!(
                "Field '{name}' type mismatch: SDK sent '{type_str}', AOT schema has '{}'",
                aot.field_type
            ));
        }
    }

    Ok(fields.len())
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

#[cfg(test)]
mod tests {
    use super::validate_register_schema;
    use crate::field_type::FieldDef;

    fn telemetry_defs() -> Vec<FieldDef> {
        vec![
            FieldDef { name: "id".into(), field_type: "string".into(), required: true },
            FieldDef { name: "source".into(), field_type: "string".into(), required: false },
            FieldDef { name: "occurred_at".into(), field_type: "long".into(), required: false },
        ]
    }

    #[test]
    fn exact_schema_match_passes() {
        let json = r#"{"table":"telemetry_events","namespace":["analytics"],"fields":[
            {"name":"id","type":"string","required":true},
            {"name":"source","type":"string","required":false},
            {"name":"occurred_at","type":"long","required":false}
        ]}"#;
        assert_eq!(validate_register_schema(json, "telemetry_events", &telemetry_defs()), Ok(3));
    }

    #[test]
    fn partial_schema_passes() {
        // SDK on older schema — engine has more fields than SDK sends. Valid: engine deployed first.
        let json = r#"{"table":"telemetry_events","namespace":["analytics"],"fields":[
            {"name":"id","type":"string","required":true}
        ]}"#;
        assert_eq!(validate_register_schema(json, "telemetry_events", &telemetry_defs()), Ok(1));
    }

    #[test]
    fn type_alias_normalised() {
        // SDK sends "int64", AOT schema says "long" — same MappedType, must pass.
        let defs = vec![FieldDef { name: "ts".into(), field_type: "long".into(), required: false }];
        let json = r#"{"table":"t","fields":[{"name":"ts","type":"int64","required":false}]}"#;
        assert_eq!(validate_register_schema(json, "t", &defs), Ok(1));
    }

    #[test]
    fn unknown_field_rejected() {
        let json = r#"{"table":"telemetry_events","fields":[
            {"name":"id","type":"string","required":true},
            {"name":"new_column","type":"string","required":false}
        ]}"#;
        let err = validate_register_schema(json, "telemetry_events", &telemetry_defs()).unwrap_err();
        assert!(err.contains("Unknown field 'new_column'"), "got: {err}");
        assert!(err.contains("engine must be deployed"), "got: {err}");
    }

    #[test]
    fn type_mismatch_rejected() {
        let json = r#"{"table":"telemetry_events","fields":[
            {"name":"occurred_at","type":"string","required":false}
        ]}"#;
        let err = validate_register_schema(json, "telemetry_events", &telemetry_defs()).unwrap_err();
        assert!(err.contains("type mismatch"), "got: {err}");
        assert!(err.contains("occurred_at"), "got: {err}");
    }

    #[test]
    fn table_mismatch_rejected() {
        let json = r#"{"table":"wrong_table","fields":[]}"#;
        let err = validate_register_schema(json, "telemetry_events", &telemetry_defs()).unwrap_err();
        assert!(err.contains("Table mismatch"), "got: {err}");
        assert!(err.contains("wrong_table"), "got: {err}");
    }

    #[test]
    fn missing_fields_array_rejected() {
        let json = r#"{"table":"telemetry_events"}"#;
        let err = validate_register_schema(json, "telemetry_events", &telemetry_defs()).unwrap_err();
        assert!(err.contains("missing 'fields'"), "got: {err}");
    }

    #[test]
    fn invalid_json_rejected() {
        let err = validate_register_schema("not json", "t", &[]).unwrap_err();
        assert!(err.contains("Invalid schema JSON"), "got: {err}");
    }
}
