//! Read and write the [GeoParquet](https://github.com/opengeospatial/geoparquet) format.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![warn(missing_docs)]

pub mod metadata;
pub mod reader;
#[cfg(test)]
mod test;
mod total_bounds;
pub mod writer;
