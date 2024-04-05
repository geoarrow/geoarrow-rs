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
//! let file = File::open("fixtures/geoparquet/nybb.parquet").unwrap();
//! let options = ParquetReaderOptions {
//!     batch_size: Some(65536),
//!     ..Default::default()
//! };
//! let output_geotable = read_geoparquet(file, options).unwrap();
//! println!("GeoTable schema: {}", output_geotable.schema());
//! ```
//!
//! ## Asynchronous reader
//!
//! ```rust
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
//! ```

mod metadata;
mod reader;
#[cfg(test)]
mod test;
mod writer;

pub use reader::{read_geoparquet, ParquetBboxPaths, ParquetReaderOptions};
#[cfg(feature = "parquet_async")]
pub use reader::{read_geoparquet_async, ParquetDataset, ParquetFile};
pub use writer::{
    write_geoparquet, GeoParquetWriter, GeoParquetWriterEncoding, GeoParquetWriterOptions,
};
#[cfg(feature = "parquet_async")]
pub use writer::{write_geoparquet_async, GeoParquetWriterAsync};
