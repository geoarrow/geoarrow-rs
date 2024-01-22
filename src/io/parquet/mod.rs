//! Read the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.

mod geoparquet_metadata;
mod reader;

pub use reader::{read_geoparquet, GeoParquetReaderOptions};
