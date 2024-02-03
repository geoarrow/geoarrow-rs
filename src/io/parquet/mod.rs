//! Read the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.

mod geoparquet_metadata;
mod reader;
#[cfg(feature = "parquet_async")]
mod reader_async;

pub use reader::{read_geoparquet, GeoParquetReaderOptions};
#[cfg(feature = "parquet_async")]
pub use reader_async::read_geoparquet_async;
