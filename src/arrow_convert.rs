use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use crate::field_type::MappedType;
use crate::protocol::FieldDef;

pub(crate) fn build_arrow_schema(fields: &[FieldDef]) -> Arc<ArrowSchema> {
    let mut next_id = 1;
    let arrow_fields = fields
        .iter()
        .map(|f| {
            let field_id = next_id;
            next_id += 1;
            let dt = MappedType::from_str_or_string(&f.field_type, &f.name).to_arrow(&mut next_id);
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            Field::new(&f.name, dt, !f.required).with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    Arc::new(ArrowSchema::new(arrow_fields))
}

/// Convert a single JSON record body to Arrow IPC Stream bytes.
/// The IPC stream is self-describing (schema embedded), so the decoder
/// does not need access to the schema registry.
pub(crate) fn json_to_ipc(
    arrow_schema: &Arc<ArrowSchema>,
    json_bytes: &[u8],
) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(json_bytes.len() + 1);
    buf.extend_from_slice(json_bytes);
    buf.push(b'\n');

    let reader = arrow_json::ReaderBuilder::new(Arc::clone(arrow_schema))
        .build(std::io::Cursor::new(buf))?;

    let mut ipc = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc, arrow_schema.as_ref())?;
        for result in reader {
            writer.write(&result?)?;
        }
        writer.finish()?;
    }
    Ok(ipc)
}

/// Decode Arrow IPC Stream bytes back into RecordBatches.
pub(crate) fn ipc_to_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = arrow::ipc::reader::StreamReader::try_new(
        std::io::Cursor::new(ipc_bytes),
        None,
    )?;
    reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::FieldDef;

    fn field(name: &str, field_type: &str, required: bool) -> FieldDef {
        FieldDef { name: name.to_string(), field_type: field_type.to_string(), required }
    }

    #[test]
    fn round_trip_single_double() {
        let schema = build_arrow_schema(&[field("value", "double", true)]);
        let ipc = json_to_ipc(&schema, br#"{"value": 3.14}"#).expect("json_to_ipc failed");
        let batches = ipc_to_batches(&ipc).expect("ipc_to_batches failed");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let col = batches[0].column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("expected Float64Array");
        assert!((col.value(0) - 3.14).abs() < 1e-9, "got {}", col.value(0));
    }

    #[test]
    fn round_trip_multiple_fields() {
        let fields = [field("x", "double", true), field("label", "string", false)];
        let schema = build_arrow_schema(&fields);
        let ipc = json_to_ipc(&schema, br#"{"x": 1.0, "label": "hello"}"#)
            .expect("json_to_ipc failed");
        let batches = ipc_to_batches(&ipc).expect("ipc_to_batches failed");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn field_id_metadata_preserved() {
        let fields = [field("a", "double", true), field("b", "long", true)];
        let schema = build_arrow_schema(&fields);
        assert_eq!(schema.field(0).metadata().get("PARQUET:field_id").map(String::as_str), Some("1"));
        assert_eq!(schema.field(1).metadata().get("PARQUET:field_id").map(String::as_str), Some("2"));
    }

    #[test]
    fn field_id_metadata_survives_ipc_round_trip() {
        let fields = [field("x", "double", true), field("y", "double", true)];
        let schema = build_arrow_schema(&fields);
        let ipc = json_to_ipc(&schema, br#"{"x": 1.0, "y": 2.0}"#).expect("json_to_ipc failed");
        let batches = ipc_to_batches(&ipc).expect("ipc_to_batches failed");
        assert_eq!(batches.len(), 1);
        let decoded_schema = batches[0].schema();
        assert_eq!(
            decoded_schema.field(0).metadata().get("PARQUET:field_id").map(String::as_str),
            Some("1"),
            "field_id for 'x' not preserved through IPC"
        );
        assert_eq!(
            decoded_schema.field(1).metadata().get("PARQUET:field_id").map(String::as_str),
            Some("2"),
            "field_id for 'y' not preserved through IPC"
        );
    }

    #[test]
    fn concat_merges_single_row_batches() {
        // The parser stage concatenates per-record 1-row batches into one batch
        // per chunk before handing them to the Parquet writer.
        let schema = build_arrow_schema(&[field("v", "double", true)]);
        let mut batches = Vec::new();
        for i in 0..3 {
            let json = format!(r#"{{"v": {}.0}}"#, i);
            let ipc = json_to_ipc(&schema, json.as_bytes()).expect("json_to_ipc failed");
            batches.extend(ipc_to_batches(&ipc).expect("ipc_to_batches failed"));
        }
        assert_eq!(batches.len(), 3);
        let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .expect("concat_batches failed");
        assert_eq!(merged.num_rows(), 3);
        assert_eq!(merged.num_columns(), 1);
    }
}
