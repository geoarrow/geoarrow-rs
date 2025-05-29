//! Read and write the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.
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
//! use geoparquet::{GeoParquetReaderOptions, GeoParquetRecordBatchReaderBuilder};
//!
//! let file = File::open("../../fixtures/geoparquet/nybb.parquet").unwrap();
//! let geo_options = GeoParquetReaderOptions::default().with_batch_size(65536);
//! // let reader_builder =
//! let reader = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
//!     file,
//!     Default::default(),
//!     geo_options,
//! )
//! .unwrap()
//! .build()
//! .unwrap();
//!
//! // The schema of the stream of record batches
//! let schema = reader.schema();
//!
//! let batches = reader
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
//! # #[cfg(feature = "async")]
//! # {
//! use geoparquet::{GeoParquetReaderOptions, GeoParquetRecordBatchStreamBuilder};
//! use tokio::fs::File;
//!
//! #[tokio::main]
//! async fn main() {
//!     let file = File::open("../../fixtures/geoparquet/nybb.parquet")
//!         .await
//!         .unwrap();
//!     let geo_options = GeoParquetReaderOptions::default().with_batch_size(65536);
//!     let reader = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
//!         file,
//!         Default::default(),
//!         geo_options,
//!     )
//!     .await
//!     .unwrap()
//!     .build()
//!     .unwrap();
//!
//!     let (batches, schema) = reader.read_table().await.unwrap();
//!     println!("Schema: {}", schema);
//!     println!("Num batches: {}", batches.len());
//! }
//! # }
//! ```

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)]

pub mod metadata;
pub mod reader;
#[cfg(test)]
mod test;
mod total_bounds;
mod writer;

pub use writer::{
    GeoParquetWriter, GeoParquetWriterEncoding, GeoParquetWriterOptions, write_geoparquet,
};
#[cfg(feature = "async")]
pub use writer::{GeoParquetWriterAsync, write_geoparquet_async};
