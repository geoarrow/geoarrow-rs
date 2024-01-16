//! Read from [newline-delimited GeoJSON](https://stevage.github.io/ndgeojson/) files.

pub mod reader;

pub use reader::read_geojson_lines;
