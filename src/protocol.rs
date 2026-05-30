use serde::{Deserialize, Serialize};

/// First byte of every IPC frame payload identifies the message type.
pub const MSG_REGISTER_SCHEMA: u8 = 0x01;
pub const MSG_INGEST_RECORD: u8 = 0x02;
pub const MSG_FLUSH_NOW: u8 = 0x03;

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
    /// Iceberg primitive type string. Accepted values (see `field_type::MappedType`):
    /// "string", "long" / "int64", "int" / "int32", "double" / "float64",
    /// "float" / "float32", "boolean", "timestamptz", "timestamp", "date",
    /// "binary" / "bytes". Unknown values are logged and treated as "string".
    ///
    /// Wire-format note for SDK authors: `timestamptz` values must be encoded
    /// in CBOR as ISO-8601 strings with a numeric UTC offset (e.g.
    /// `2026-05-30T07:30:02.123456Z` or `…+00:00`). Named timezones like
    /// `"UTC"` are NOT accepted by the engine's arrow-json parser unless
    /// arrow's `chrono-tz` feature is enabled.
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
///   [CBOR stream bytes]   (Remaining bytes, CBOR serialized payload)
///
/// This eliminates the need for a JSON `IngestRecord` struct, as the engine
/// passes the raw bytes directly to RocksDB.

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
