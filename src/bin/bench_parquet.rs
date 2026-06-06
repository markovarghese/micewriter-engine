use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let num_rows = 1_000_000;
    
    println!("Generating {} rows of dummy Arrow data...", num_rows);
    let start_gen = Instant::now();

    let timestamps = Arc::new(TimestampMicrosecondArray::from_iter_values(
        (0..num_rows).map(|i| 1700000000000000 + i as i64),
    )) as ArrayRef;
    
    let event_ids = Arc::new(StringArray::from_iter_values(
        (0..num_rows).map(|i| format!("evt_{:010}", i)),
    )) as ArrayRef;

    let user_ids = Arc::new(Int64Array::from_iter_values(
        (0..num_rows).map(|i| (i % 10000) as i64),
    )) as ArrayRef;

    let session_ids = Arc::new(StringArray::from_iter_values(
        (0..num_rows).map(|i| format!("sess_{:010}", i % 50000)),
    )) as ArrayRef;

    let amounts = Arc::new(Int64Array::from_iter_values(
        (0..num_rows).map(|i| (i % 100) as i64),
    )) as ArrayRef;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![timestamps, event_ids, user_ids, session_ids, amounts],
    )
    .unwrap();
    
    println!("Generation took: {:?}", start_gen.elapsed());
    
    // Calculate total raw bytes in the batch
    let batch_bytes = batch.get_array_memory_size();
    println!("Raw Arrow batch size: {:.2} MB", batch_bytes as f64 / 1_048_576.0);

    let props = Arc::new(WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build());

    println!("Starting Parquet SNAPPY compression benchmark...");
    let start_comp = Instant::now();

    let mut writer = ArrowWriter::try_new(vec![], schema, Some(props.as_ref().clone())).unwrap();
    writer.write(&batch).unwrap();
    let out = writer.into_inner().unwrap();

    let elapsed = start_comp.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let throughput = (batch_bytes as f64 / 1_048_576.0) / elapsed_secs;

    println!("Compression took: {:?}", elapsed);
    println!("Output Parquet size: {:.2} MB", out.len() as f64 / 1_048_576.0);
    println!("Parquet Compression Throughput: {:.2} MB/s", throughput);
}
