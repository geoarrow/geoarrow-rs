//! Read the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.

pub mod geoparquet_metadata;
pub mod reader;

pub use reader::read_geoparquet;
