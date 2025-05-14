APIs for reading GeoParquet data into Arrow [`RecordBatch`es][arrow_array::RecordBatch].

Both synchronous and asynchronous APIs are provided, with the latter being enabled by the `async` feature flag.

## Synchronous reader

```rust
# #[cfg(feature = "compression")]
# {
use std::fs::File;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_schema::ArrowError;
use geoarrow_geoparquet::{GeoParquetReaderOptions, GeoParquetRecordBatchReaderBuilder};

let file = File::open("../../fixtures/geoparquet/nybb.parquet").unwrap();
let geo_options = GeoParquetReaderOptions::default().with_batch_size(65536);
let reader = GeoParquetRecordBatchReaderBuilder::try_new(
    file,
    Default::default(),
    geo_options,
)
.unwrap()
.build()
.unwrap();

// The schema of the stream of record batches
let schema = reader.schema();

let batches = reader
    .collect::<Result<Vec<RecordBatch>, ArrowError>>()
    .unwrap();
println!("Schema: {}", schema);
println!("Num batches: {}", batches.len());
# }
```

## Asynchronous reader

Thsi requires the `async` feature flag to be enabled.

```rust
# #[cfg(feature = "async")]
# {
use geoarrow_geoparquet::{GeoParquetReaderOptions, GeoParquetRecordBatchStreamBuilder};
use tokio::fs::File;

#[tokio::main]
async fn main() {
    let file = File::open("../../fixtures/geoparquet/nybb.parquet")
        .await
        .unwrap();
    let geo_options = GeoParquetReaderOptions::default().with_batch_size(65536);
    let reader = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
        file,
        Default::default(),
        geo_options,
    )
    .await
    .unwrap()
    .build()
    .unwrap();

let (batches, schema) = reader.read_table().await.unwrap();
    println!("Schema: {}", schema);
    println!("Num batches: {}", batches.len());
}
# }
```
