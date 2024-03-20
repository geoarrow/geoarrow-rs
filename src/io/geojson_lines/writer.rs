use geozero::geojson::GeoJsonLineWriter;
use geozero::GeozeroDatasource;
use std::io::Write;

use crate::error::Result;
use crate::table::GeoTable;

/// Write a table to newline-delimited GeoJSON
pub fn write_geojson_lines<W: Write>(table: &mut GeoTable, writer: W) -> Result<()> {
    let mut geojson_writer = GeoJsonLineWriter::new(writer);
    table.process(&mut geojson_writer)?;
    Ok(())
}
