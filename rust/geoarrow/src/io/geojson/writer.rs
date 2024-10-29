use crate::error::Result;
use crate::io::stream::RecordBatchReader;
use geozero::geojson::GeoJsonWriter;
use geozero::GeozeroDatasource;
use std::io::Write;

/// Write a Table to GeoJSON
///
/// Note: Does not reproject to WGS84 for you
pub fn write_geojson<W: Write, S: Into<RecordBatchReader>>(stream: S, writer: W) -> Result<()> {
    let mut geojson = GeoJsonWriter::new(writer);
    stream.into().process(&mut geojson)?;
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
        write_geojson(&table, writer).unwrap();
        let output_string = String::from_utf8(output_buffer).unwrap();
        println!("{}", output_string);
    }
}
