use serde::{Deserialize, Serialize};

/// First byte of every IPC frame payload identifies the message type.
pub const MSG_REGISTER_SCHEMA: u8 = 0x01;
pub const MSG_INGEST_RECORD: u8 = 0x02;

// ---------------------------------------------------------------------------
// Inbound messages (Java SDK → Engine)
// ---------------------------------------------------------------------------

/// Sent once per annotated entity class on SDK startup.
/// Payload encoding: JSON (type discriminant byte + JSON body).
#[derive(Debug, Clone, Deserialize)]
pub struct RegisterSchema {
    pub table: String,
    /// Iceberg namespace path components, e.g. ["micewriter"].
    pub namespace: Vec<String>,
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FieldDef {
    pub name: String,
    /// Iceberg primitive type string: "string", "long", "int", "double",
    /// "float", "boolean", "timestamptz", "timestamp", "date", "binary".
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default = "bool_true")]
    pub required: bool,
}

fn bool_true() -> bool {
    true
}

/// Hot-path telemetry record. One per `.send(pojo)` call.
/// Payload encoding: JSON (same framing as schema, type discriminant differs).
///
/// Note: switching to Protobuf/Bincode here is a future optimisation.
/// The engine currently stores the raw JSON bytes in RocksDB as-is.
#[derive(Debug, Deserialize, Serialize)]
pub struct IngestRecord {
    pub table: String,
    /// Ordered list of (field_name, value) pairs matching the registered schema.
    pub fields: Vec<(String, serde_json::Value)>,
}

// ---------------------------------------------------------------------------
// Outbound messages (Engine → Java SDK)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct AckResponse {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg: Option<String>,
}

impl AckResponse {
    pub fn ok() -> Self {
        Self { status: "ok", msg: None }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self { status: "error", msg: Some(msg.into()) }
    }
}
