//! Read and write the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.

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
