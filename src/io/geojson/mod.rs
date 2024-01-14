//! Contains implementations of reading from and writing to [GeoJSON](https://geojson.org/) files.

pub use reader::read_geojson;
pub use writer::write_geojson;

mod reader;
mod writer;
