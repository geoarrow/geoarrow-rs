//! Read the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.
//!
//!# Examples of reading GeoParquet file into a GeoTable
//!
//!## Synchronous reader
//!
//!```rust
//! use geoarrow::io::parquet::read_geoparquet;
//! use geoarrow::io::parquet::GeoParquetReaderOptions;
//! use std::fs::File;
//!
//! let file = File::open("nybb.parquet").unwrap();
//! let options = GeoParquetReaderOptions::new(65536, Default::default());
//! let output_geotable = read_geoparquet(file, options).unwrap();
//! println!("GeoTable schema: {}", output_geotable.schema());
//!```
//!
//!## Asynchronous reader
//!
//!```rust
//! use geoarrow::io::parquet::read_geoparquet_async;
//! use geoarrow::io::parquet::GeoParquetReaderOptions;
//! use tokio::fs::File;
//!
//! #[tokio::main]
//! async fn main() {
//!     let file = File::open("fixtures/geoparquet/nybb.parquet")
//!         .await
//!         .unwrap();
//!     let options = GeoParquetReaderOptions::new(65536, Default::default());
//!     let output_geotable = read_geoparquet_async(file, options).await.unwrap();
//!     println!("GeoTable schema: {}", output_geotable.schema());
//! }
//!```

mod geoparquet_metadata;
mod reader;
#[cfg(feature = "parquet_async")]
mod reader_async;

pub use reader::{read_geoparquet, GeoParquetReaderOptions};
#[cfg(feature = "parquet_async")]
pub use reader_async::read_geoparquet_async;
