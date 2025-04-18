use geozero::GeozeroDatasource;
use geozero::geojson::GeoJsonLineWriter;
use std::io::Write;

use crate::error::Result;
use crate::io::stream::RecordBatchReader;

/// Write a table to newline-delimited GeoJSON
pub fn write_geojson_lines<W: Write, S: Into<RecordBatchReader>>(
    stream: S,
    writer: W,
) -> Result<()> {
    let mut geojson_writer = GeoJsonLineWriter::new(writer);
    stream.into().process(&mut geojson_writer)?;
    Ok(())
}
