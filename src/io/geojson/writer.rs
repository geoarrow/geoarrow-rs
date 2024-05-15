use super::geojson_writer::GeoJsonWriter;
use crate::error::Result;
use crate::io::stream::RecordBatchReader;
use geozero::GeozeroDatasource;
use std::io::Write;

/// Write a Table to GeoJSON
///
/// Note: Does not reproject to WGS84 for you
pub fn write_geojson<W: Write>(table: &mut RecordBatchReader, writer: W) -> Result<()> {
    let mut geojson = GeoJsonWriter::new(writer);
    table.process(&mut geojson)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point;
    use std::io::BufWriter;

    #[test]
    fn test_write() {
        let table = point::table();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_geojson(&mut table.into(), writer).unwrap();
        let output_string = String::from_utf8(output_buffer).unwrap();
        println!("{}", output_string);
    }
}
