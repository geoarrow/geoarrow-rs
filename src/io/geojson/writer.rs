use crate::table::GeoTable;
use geozero::error::GeozeroError;
use geozero::geojson::GeoJsonWriter;
use geozero::GeozeroDatasource;
use std::io::Write;

/// Write a GeoTable to GeoJSON
///
/// Note: Does not reproject to WGS84 for you
pub fn write_geojson<W: Write>(table: &mut GeoTable, writer: W) -> Result<(), GeozeroError> {
    let mut geojson = GeoJsonWriter::new(writer);
    table.process(&mut geojson)?;
    Ok(())
}
