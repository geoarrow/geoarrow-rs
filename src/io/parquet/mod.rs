//! Read the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.
//!
//! # Examples of reading GeoParquet file into a GeoTable
//!
//! ## Synchronous reader
//!
//! ```rust
//! use geoarrow::io::parquet::read_geoparquet;
//! use geoarrow::io::parquet::ParquetReaderOptions;
//! use std::fs::File;
//!
//! # #[cfg(feature = "parquet_compression")]
//! # {
//! let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
//! let options = ParquetReaderOptions {
//!     batch_size: Some(65536),
//!     ..Default::default()
//! };
//! let output_geotable = read_geoparquet(file, options).unwrap();
//! println!("GeoTable schema: {}", output_geotable.schema());
//! # }
//! ```
//!
//! ## Asynchronous reader
//!
//! ```rust
//! # #[cfg(feature = "parquet_async")]
//! # {
//! use geoarrow::io::parquet::read_geoparquet_async;
//! use geoarrow::io::parquet::ParquetReaderOptions;
//! use tokio::fs::File;
//!
//! #[tokio::main]
//! async fn main() {
//!     let file = File::open("fixtures/geoparquet/nybb.parquet")
//!         .await
//!         .unwrap();
//!     let options = ParquetReaderOptions {
//!         batch_size: Some(65536),
//!         ..Default::default()
//!     };
//!     let output_geotable = read_geoparquet_async(file, options).await.unwrap();
//!     println!("GeoTable schema: {}", output_geotable.schema());
//! }
//! # }
//! ```

mod metadata;
mod reader;
#[cfg(test)]
mod test;
mod writer;

pub use reader::{
    GeoParquetReaderMetadata, GeoParquetReaderOptions, GeoParquetRecordBatchReader,
    GeoParquetRecordBatchReaderBuilder, ParquetBboxPaths,
};
#[cfg(feature = "parquet_async")]
pub use reader::{GeoParquetRecordBatchStream, GeoParquetRecordBatchStreamBuilder};
pub use writer::{
    write_geoparquet, GeoParquetWriter, GeoParquetWriterEncoding, GeoParquetWriterOptions,
};
#[cfg(feature = "parquet_async")]
pub use writer::{write_geoparquet_async, GeoParquetWriterAsync};
