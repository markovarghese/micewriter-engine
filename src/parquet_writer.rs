use anyhow::Result;

use crate::protocol::FieldDef;

/// Compile a batch of raw Arrow IPC payloads (all belonging to the same table) into
/// a single Parquet file bytes array.
pub fn compile(records: &[Vec<u8>], _field_defs: &[FieldDef]) -> Result<Vec<u8>> {
    if records.is_empty() {
        return Ok(vec![]);
    }

    let props = parquet::file::properties::WriterProperties::builder().build();
    let mut writer: Option<parquet::arrow::ArrowWriter<Vec<u8>>> = None;

    for record_bytes in records {
        if record_bytes.len() < 2 {
            continue;
        }
        
        let table_len = u16::from_be_bytes([record_bytes[0], record_bytes[1]]) as usize;
        let header_len = 2 + table_len + 4;
        
        if record_bytes.len() <= header_len {
            continue;
        }
        
        let ipc_bytes = &record_bytes[header_len..];
        let cursor = std::io::Cursor::new(ipc_bytes);
        
        let reader = match arrow::ipc::reader::StreamReader::try_new(cursor, None) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to read Arrow IPC stream: {}", e);
                continue;
            }
        };
        
        if writer.is_none() {
            let schema = reader.schema();
            writer = Some(parquet::arrow::ArrowWriter::try_new(vec![], schema, Some(props.clone()))?);
        }
        
        let w = writer.as_mut().unwrap();
        
        for batch_result in reader {
            match batch_result {
                Ok(batch) => {
                    w.write(&batch)?;
                }
                Err(e) => {
                    tracing::warn!("Failed to read Arrow batch: {}", e);
                }
            }
        }
    }
    
    if let Some(w) = writer {
        let buf = w.into_inner()?;
        return Ok(buf);
    }

    Ok(vec![])
}
