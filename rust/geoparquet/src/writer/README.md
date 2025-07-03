Write GeoParquet data from GeoArrow.

This module provides the ability to write GeoParquet files from
[`RecordBatch`][arrow_array::RecordBatch]es with GeoArrow metadata.

The primary writing API is [`GeoParquetRecordBatchEncoder`], which prepares
GeoArrow [`RecordBatch`][arrow_array::RecordBatch]es to be written via the
upstream [`parquet`] writer APIs. The [`GeoParquetRecordBatchEncoder`] does not
handle the actual writing; it only transforms the `RecordBatch` and manages the
construction of GeoParquet metadata.

## Overview

To write to a Parquet file:

1. Create a [`GeoParquetRecordBatchEncoder`].
2. Create an upstream [`ArrowWriter`][parquet::arrow::arrow_writer::ArrowWriter] or [`AsyncArrowWriter`][parquet::arrow::async_writer::AsyncArrowWriter]. Ensure you pass the output of [`GeoParquetRecordBatchEncoder::target_schema`] as the schema to the writer.
3. For each Arrow [`RecordBatch`][arrow_array::RecordBatch] you want to write, call [`GeoParquetRecordBatchEncoder::encode_record_batch`] to encode the batch, then pass the output to [`ArrowWriter::write`][parquet::arrow::arrow_writer::ArrowWriter::write] or [`AsyncArrowWriter::write`][parquet::arrow::async_writer::AsyncArrowWriter::write].
4. Before you close the Parquet writer, call [`GeoParquetRecordBatchEncoder::into_keyvalue`] to construct the GeoParquet metadata for the file. Then call [`ArrowWriter::append_key_value_metadata`][parquet::arrow::arrow_writer::ArrowWriter::append_key_value_metadata] or [`AsyncArrowWriter::append_key_value_metadata`][parquet::arrow::async_writer::AsyncArrowWriter::append_key_value_metadata] to append the key-value metadata to the Parquet file.

## Synchronous writer

```rust
# use std::io::Write;
#
# use arrow_array::RecordBatch;
# use arrow_schema::Schema;
# use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
# use parquet::arrow::ArrowWriter;
#
# fn tmp<W: Write + Send>(
#     file: W,
#     schema: Schema,
#     options: GeoParquetWriterOptions,
#     input_batches: Vec<RecordBatch>,
# ) {
let mut gpq_encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options).unwrap();
let mut parquet_writer = ArrowWriter::try_new(file, gpq_encoder.target_schema(), None).unwrap();

for batch in input_batches {
    let encoded_batch = gpq_encoder.encode_record_batch(&batch).unwrap();
    parquet_writer.write(&encoded_batch).unwrap();
}

let kv_metadata = gpq_encoder.into_keyvalue().unwrap();
parquet_writer.append_key_value_metadata(kv_metadata);
parquet_writer.finish().unwrap();
# }
```

## Asynchronous writer

```rust
# use arrow_array::RecordBatch;
# use arrow_schema::Schema;
# use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
# use parquet::arrow::AsyncArrowWriter;
# use parquet::arrow::async_writer::AsyncFileWriter;
#
# async fn tmp<W: AsyncFileWriter>(
#     file: W,
#     schema: Schema,
#     options: GeoParquetWriterOptions,
#     input_batches: Vec<RecordBatch>,
# ) {
let mut gpq_encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options).unwrap();
let mut parquet_writer =
  AsyncArrowWriter::try_new(file, gpq_encoder.target_schema(), None).unwrap();

for batch in input_batches {
    let encoded_batch = gpq_encoder.encode_record_batch(&batch).unwrap();
    parquet_writer.write(&encoded_batch).await.unwrap();
}

let kv_metadata = gpq_encoder.into_keyvalue().unwrap();
parquet_writer.append_key_value_metadata(kv_metadata);
parquet_writer.finish().await.unwrap();
# }
```
