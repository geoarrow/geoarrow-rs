Read GeoParquet data as GeoArrow.

The APIs in this crate

into Arrow [`RecordBatch`][arrow_array::RecordBatch]es with
GeoArrow metadata.

## Overview

The general overview of reading a GeoParquet file is as follows:

1. Create an upstream [`ParquetRecordBatchReaderBuilder`][parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder] or [`ParquetRecordBatchStreamBuilder`][parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder] depending on whether you want synchronous or asynchronous reading, respectively.
2. Use [`GeoParquetReaderBuilder::geoparquet_metadata`] to parse the GeoParquet metadata from the Parquet [`FileMetaData`][parquet::file::metadata::FileMetaData].
3. Use [`GeoParquetReaderBuilder::geoarrow_schema`] to infer a GeoArrow schema given the GeoParquet metadata. This step gives you the choice of whether geometries should be parsed to their GeoArrow-native representation or whether they should be left as WKB.

    Even if you plan to leave geometries as WKB, you still need to call this method, as it will ensure the GeoArrow metadata is applied to the WKB geometry column.

4. (Optional) Use the [`GeoParquetReaderBuilder`] trait to inject any spatial filter functionality onto the reader.
5. Call `build` to get a [reader][parquet::arrow::arrow_reader::ParquetRecordBatchReader] or [stream][parquet::arrow::async_reader::ParquetRecordBatchStream]. Then wrap that with a [`GeoParquetRecordBatchReader`][geoparquet::reader::GeoParquetRecordBatchReader] or [`GeoParquetRecordBatchStream`][geoparquet::reader::GeoParquetRecordBatchStream], including the inferred GeoArrow schema from step 3.

    Now any [`RecordBatch`][arrow_array::RecordBatch]es emitted by the reader or stream will have GeoArrow metadata on each geometry column. And if, in step 3, you chose to parse geometries to their GeoArrow-native representation, these geospatial-aware wrappers will parse the WKB column to a GeoArrow-native representation.

## Synchronous reader

```rust
# #[cfg(feature = "compression")]
# {
use std::fs::File;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

let file = File::open("../../fixtures/geoparquet/nybb.parquet").unwrap();
let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

let geoparquet_metadata = builder.geoparquet_metadata().unwrap();
let geoarrow_schema = builder
    .geoarrow_schema(&geoparquet_metadata, true, Default::default())
    .unwrap();

let parquet_reader = builder.with_batch_size(65536).build().unwrap();
let geoparquet_reader =
    GeoParquetRecordBatchReader::try_new(parquet_reader, geoarrow_schema).unwrap();

// The schema of the stream of record batches
let schema = geoparquet_reader.schema();
let batches = geoparquet_reader
    .collect::<Result<Vec<RecordBatch>, ArrowError>>()
    .unwrap();
println!("Schema: {}", schema);
println!("Num batches: {}", batches.len());
# }
```

### Reading with spatial filter

Refer to [`GeoParquetReaderBuilder`] for how to construct spatial filters and set them on an
[`ArrowReaderBuilder`][parquet::arrow::arrow_reader::ArrowReaderBuilder].

## Asynchronous reader

```rust
# #[cfg(all(feature = "compression", feature = "async"))]
# {
use futures::TryStreamExt;
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchStream};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

# tokio_test::block_on(async {
let file = File::open("../../fixtures/geoparquet/nybb.parquet")
    .await
    .unwrap();
let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();

let geoparquet_metadata = builder.geoparquet_metadata().unwrap();
let geoarrow_schema = builder
    .geoarrow_schema(&geoparquet_metadata, true, Default::default())
    .unwrap();

let parquet_stream = builder.with_batch_size(65536).build().unwrap();
let geoparquet_stream =
    GeoParquetRecordBatchStream::try_new(parquet_stream, geoarrow_schema).unwrap();

// The schema of the stream of record batches
let schema = geoparquet_stream.schema();
let batches: Vec<_> = geoparquet_stream.try_collect().await.unwrap();
println!("Schema: {}", schema);
println!("Num batches: {}", batches.len());
# })
# }
```
