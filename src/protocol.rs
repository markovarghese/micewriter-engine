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
    #[allow(dead_code)] // part of IPC wire protocol; reserved for future use
    pub schema_id: Option<i32>,
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

/// Hot-path telemetry record stream.
/// Payload encoding: Custom Binary Framing.
/// 
/// The `MSG_INGEST_RECORD` payload (everything after the 4-byte length and 1-byte discriminant) 
/// is structured as follows:
///   [table_name_len: u16] (2 bytes, big-endian)
///   [table_name_bytes]    (UTF-8 string)
///   [schema_id: i32]      (4 bytes, big-endian)
///   [Arrow IPC Stream]    (Remaining bytes, raw native Apache Arrow IPC stream format)
///
/// This eliminates the need for a JSON `IngestRecord` struct, as the engine
/// passes the raw Arrow bytes directly to RocksDB and then directly to the Parquet compiler.

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
