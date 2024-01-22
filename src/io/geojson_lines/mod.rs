//! Read from [newline-delimited GeoJSON](https://stevage.github.io/ndgeojson/) files.

mod reader;

pub use reader::read_geojson_lines;
