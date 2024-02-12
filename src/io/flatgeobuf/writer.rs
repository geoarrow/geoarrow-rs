use std::io::Write;

use flatgeobuf::{FgbWriter, FgbWriterOptions};
use geozero::GeozeroDatasource;

use crate::error::GeoArrowError;
use crate::table::GeoTable;

// TODO: always write CRS saved in GeoTable metadata (you can do this by adding an option)
/// Write a GeoTable to a FlatGeobuf file.
pub fn write_flatgeobuf<W: Write>(
    table: &mut GeoTable,
    writer: W,
    name: &str,
) -> Result<(), GeoArrowError> {
    write_flatgeobuf_with_options(table, writer, name, Default::default())
}

/// Write a GeoTable to a FlatGeobuf file with specific writer options.
///
/// Note: this `name` argument is what OGR observes as the layer name of the file.
pub fn write_flatgeobuf_with_options<W: Write>(
    table: &mut GeoTable,
    writer: W,
    name: &str,
    options: FgbWriterOptions,
) -> Result<(), GeoArrowError> {
    let mut fgb =
        FgbWriter::create_with_options(name, infer_flatgeobuf_geometry_type(table), options)?;
    table.process(&mut fgb)?;
    fgb.write(writer)?;
    Ok(())
}

fn infer_flatgeobuf_geometry_type(table: &GeoTable) -> flatgeobuf::GeometryType {
    let fields = &table.schema().fields;
    let geometry_field = &fields[table.geometry_column_index()];
    if let Some(extension_name) = geometry_field.metadata().get("ARROW:extension:name") {
        let geometry_type = match extension_name.as_str() {
            "geoarrow.point" => flatgeobuf::GeometryType::Point,
            "geoarrow.linestring" => flatgeobuf::GeometryType::LineString,
            "geoarrow.polygon" => flatgeobuf::GeometryType::Polygon,
            "geoarrow.multipoint" => flatgeobuf::GeometryType::MultiPoint,
            "geoarrow.multilinestring" => flatgeobuf::GeometryType::MultiLineString,
            "geoarrow.multipolygon" => flatgeobuf::GeometryType::MultiPolygon,
            "geoarrow.geometry" => flatgeobuf::GeometryType::Unknown,
            "geoarrow.geometrycollection" => flatgeobuf::GeometryType::GeometryCollection,
            _ => todo!(),
        };
        geometry_type
    } else {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::flatgeobuf::read_flatgeobuf;
    use crate::test::point;
    use std::io::{BufWriter, Cursor};

    #[test]
    fn test_write() {
        let mut table = point::table();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_flatgeobuf(&mut table, writer, "name").unwrap();

        let mut reader = Cursor::new(output_buffer);
        let new_table = read_flatgeobuf(&mut reader, Default::default(), None).unwrap();

        // TODO: it looks like it's getting read back in backwards row order!
        let batch = &new_table.batches()[0];
        let arr = batch.column(0);
        dbg!(arr);
        dbg!(new_table);
        // dbg!(output_buffer);
    }
}
