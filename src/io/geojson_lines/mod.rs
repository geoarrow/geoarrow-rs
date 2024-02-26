//! Read from and write to [newline-delimited GeoJSON](https://stevage.github.io/ndgeojson/) files.

mod reader;
mod writer;

pub use reader::read_geojson_lines;
pub use writer::write_geojson_lines;
