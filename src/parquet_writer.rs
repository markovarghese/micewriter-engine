use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::protocol::{FieldDef, IngestRecord};

/// Compile a batch of `IngestRecord`s (all belonging to the same table) into
/// Parquet bytes, using `field_defs` to determine column types.
pub fn compile(records: &[IngestRecord], field_defs: &[FieldDef]) -> Result<Vec<u8>> {
    if records.is_empty() {
        return Ok(vec![]);
    }

    let arrow_schema = Arc::new(build_arrow_schema(field_defs));

    // Build one Vec per column from the record fields.
    let arrays = build_arrays(records, field_defs, &arrow_schema)?;
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;

    let mut buf: Vec<u8> = vec![];
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(buf)
}

/// Map IPC field type strings to Arrow DataTypes.
fn iceberg_type_to_arrow(type_str: &str) -> DataType {
    match type_str {
        "string" => DataType::Utf8,
        "long" | "int64" => DataType::Int64,
        "int" | "int32" => DataType::Int32,
        "double" | "float64" => DataType::Float64,
        "float" | "float32" => DataType::Float32,
        "boolean" => DataType::Boolean,
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "date" => DataType::Date32,
        "binary" | "bytes" => DataType::LargeBinary,
        _ => DataType::Utf8, // safe fallback
    }
}

fn build_arrow_schema(fields: &[FieldDef]) -> ArrowSchema {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| Field::new(&f.name, iceberg_type_to_arrow(&f.field_type), !f.required))
        .collect();
    ArrowSchema::new(arrow_fields)
}

fn build_arrays(
    records: &[IngestRecord],
    field_defs: &[FieldDef],
    schema: &ArrowSchema,
) -> Result<Vec<ArrayRef>> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(field_defs.len());

    for (col_idx, field_def) in field_defs.iter().enumerate() {
        let arrow_field = schema.field(col_idx);
        let col_values: Vec<Option<&serde_json::Value>> = records
            .iter()
            .map(|r| r.fields.iter().find(|(k, _)| k == &field_def.name).map(|(_, v)| v))
            .collect();

        let array: ArrayRef = match arrow_field.data_type() {
            DataType::Utf8 => {
                let vals: Vec<Option<&str>> = col_values
                    .iter()
                    .map(|v| v.and_then(|j| j.as_str()))
                    .collect();
                Arc::new(StringArray::from(vals))
            }
            DataType::Int64 => {
                let vals: Vec<Option<i64>> = col_values
                    .iter()
                    .map(|v| v.and_then(|j| j.as_i64()))
                    .collect();
                Arc::new(Int64Array::from(vals))
            }
            DataType::Int32 => {
                let vals: Vec<Option<i32>> = col_values
                    .iter()
                    .map(|v| v.and_then(|j| j.as_i64().map(|n| n as i32)))
                    .collect();
                Arc::new(Int32Array::from(vals))
            }
            DataType::Float64 => {
                let vals: Vec<Option<f64>> = col_values
                    .iter()
                    .map(|v| v.and_then(|j| j.as_f64()))
                    .collect();
                Arc::new(Float64Array::from(vals))
            }
            DataType::Boolean => {
                let vals: Vec<Option<bool>> = col_values
                    .iter()
                    .map(|v| v.and_then(|j| j.as_bool()))
                    .collect();
                Arc::new(BooleanArray::from(vals))
            }
            // Timestamps: expect the SDK to send microseconds-since-epoch as i64.
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let vals: Vec<Option<i64>> = col_values
                    .iter()
                    .map(|v| v.and_then(|j| j.as_i64()))
                    .collect();
                Arc::new(
                    arrow::array::TimestampMicrosecondArray::from(vals)
                        .with_timezone_opt(match arrow_field.data_type() {
                            DataType::Timestamp(_, tz) => tz.clone(),
                            _ => None,
                        }),
                )
            }
            DataType::Date32 => {
                let vals: Vec<Option<i32>> = col_values
                    .iter()
                    .map(|v| v.and_then(|j| j.as_i64().map(|n| n as i32)))
                    .collect();
                Arc::new(arrow::array::Date32Array::from(vals))
            }
            // Fallback: everything else as UTF-8 string.
            _ => {
                let vals: Vec<Option<String>> = col_values
                    .iter()
                    .map(|v| v.map(|j| j.to_string()))
                    .collect();
                Arc::new(StringArray::from(
                    vals.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                ))
            }
        };

        arrays.push(array);
    }

    Ok(arrays)
}
