//! Read and write the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.
//!
//! # Examples of reading GeoParquet file into a GeoTable
//!
//! ## Synchronous reader
//!
//! ```rust
//! use geoarrow::io::parquet::{GeoParquetReaderOptions, GeoParquetRecordBatchReaderBuilder};
//! use std::fs::File;
//!
//! # #[cfg(feature = "parquet_compression")]
//! # {
//! let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
//! let geo_options = GeoParquetReaderOptions::default().with_batch_size(65536);
//! let table = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
//!     file,
//!     Default::default(),
//!     geo_options,
//! )
//! .unwrap()
//! .build()
//! .unwrap()
//! .read_table()
//! .unwrap();
//! println!("Table schema: {}", table.schema());
//! # }
//! ```
//!
//! ## Asynchronous reader
//!
//! ```rust
//! # #[cfg(feature = "parquet_async")]
//! # {
//! use geoarrow::io::parquet::{GeoParquetReaderOptions, GeoParquetRecordBatchStreamBuilder};
//! use tokio::fs::File;
//!
//! #[tokio::main]
//! async fn main() {
//!     let file = File::open("fixtures/geoparquet/nybb.parquet")
//!         .await
//!         .unwrap();
//!     let geo_options = GeoParquetReaderOptions::default().with_batch_size(65536);
//!     let table = GeoParquetRecordBatchStreamBuilder::try_new_with_options(
//!         file,
//!         Default::default(),
//!         geo_options,
//!     )
//!     .await
//!     .unwrap()
//!     .build()
//!     .unwrap()
//!     .read_table()
//!     .await
//!     .unwrap();
//!     println!("Table schema: {}", table.schema());
//! }
//! # }
//! ```

#![deny(missing_docs)]

pub mod metadata;
mod reader;
#[cfg(test)]
mod test;
mod writer;

pub use reader::{
    expand_glob, GeoParquetDatasetMetadata, GeoParquetReaderMetadata, GeoParquetReaderOptions,
    GeoParquetRecordBatchReader, GeoParquetRecordBatchReaderBuilder,
};
#[cfg(feature = "parquet_async")]
pub use reader::{GeoParquetRecordBatchStream, GeoParquetRecordBatchStreamBuilder};
pub use writer::{
    write_geoparquet, GeoParquetWriter, GeoParquetWriterEncoding, GeoParquetWriterOptions,
};
#[cfg(feature = "parquet_async")]
pub use writer::{write_geoparquet_async, GeoParquetWriterAsync};
