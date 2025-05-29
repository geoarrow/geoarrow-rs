//! Read the GeoParquet file format into Arrow [`RecordBatch`][arrow_array::RecordBatch]es with
//! GeoArrow metadata.
//!
//! # Examples of reading GeoParquet file into a GeoTable
//!
//! ## Synchronous reader
//!
//! ```rust
//! # #[cfg(feature = "compression")]
//! # {
//! use std::fs::File;
//!
//! use arrow_array::RecordBatch;
//! use arrow_array::RecordBatchReader;
//! use arrow_schema::ArrowError;
//! use geoarrow_schema::CoordType;
//! use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
//! use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
//!
//! let file = File::open("../../fixtures/geoparquet/nybb.parquet").unwrap();
//! let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
//!
//! let gpq_meta = builder.geoparquet_metadata().unwrap();
//! let parse_to_native = true;
//! let geoarrow_schema = builder
//!     .geoarrow_schema(&gpq_meta, parse_to_native, CoordType::Separated)
//!     .unwrap();
//!
//! let parquet_reader = builder.with_batch_size(65536).build().unwrap();
//! let geoparquet_reader =
//!     GeoParquetRecordBatchReader::try_new(parquet_reader, geoarrow_schema).unwrap();
//!
//! // The schema of the stream of record batches
//! let schema = geoparquet_reader.schema();
//! let batches = geoparquet_reader
//!     .collect::<Result<Vec<RecordBatch>, ArrowError>>()
//!     .unwrap();
//! println!("Schema: {}", schema);
//! println!("Num batches: {}", batches.len());
//! # }
//! ```
//!
//! ## Asynchronous reader
//!
//! ```rust
//! # #[cfg(all(feature = "compression", feature = "async"))]
//! # {
//! use futures::TryStreamExt;
//! use geoarrow_schema::CoordType;
//! use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchStream};
//! use parquet::arrow::ParquetRecordBatchStreamBuilder;
//! use tokio::fs::File;
//!
//! # tokio_test::block_on(async {
//! let file = File::open("../../fixtures/geoparquet/nybb.parquet")
//!     .await
//!     .unwrap();
//! let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();
//!
//! let gpq_meta = builder.geoparquet_metadata().unwrap();
//! let parse_to_native = true;
//! let geoarrow_schema = builder
//!     .geoarrow_schema(&gpq_meta, parse_to_native, CoordType::Separated)
//!     .unwrap();
//!
//! let parquet_stream = builder.with_batch_size(65536).build().unwrap();
//! let geoparquet_stream =
//!     GeoParquetRecordBatchStream::try_new(parquet_stream, geoarrow_schema).unwrap();
//!
//! // The schema of the stream of record batches
//! let schema = geoparquet_stream.schema();
//! let batches: Vec<_> = geoparquet_stream.try_collect().await.unwrap();
//! println!("Schema: {}", schema);
//! println!("Num batches: {}", batches.len());
//! # })
//! # }
//! ```

#[cfg(feature = "async")]
mod r#async;
mod geo_ext;
mod metadata;
mod parse;
mod spatial_filter;
mod sync;

#[cfg(feature = "async")]
pub use r#async::GeoParquetRecordBatchStream;
pub use geo_ext::GeoParquetReaderBuilder;
pub use metadata::{GeoParquetDatasetMetadata, GeoParquetReaderMetadata};
pub use sync::GeoParquetRecordBatchReader;
