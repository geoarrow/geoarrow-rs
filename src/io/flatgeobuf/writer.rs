use std::io::Write;

use flatgeobuf::{FgbWriter, FgbWriterOptions};
use geozero::GeozeroDatasource;

use crate::table::GeoTable;

// TODO: always write CRS saved in GeoTable metadata (you can do this by adding an option)
pub fn write_flatgeobuf<W: Write>(table: &mut GeoTable, writer: W, name: &str) {
    let mut fgb = FgbWriter::create(name, infer_flatgeobuf_geometry_type(table)).unwrap();
    table.process(&mut fgb).unwrap();
    fgb.write(writer).unwrap();
}

pub fn write_flatgeobuf_with_options<W: Write>(
    table: &mut GeoTable,
    writer: W,
    name: &str,
    options: FgbWriterOptions,
) {
    let mut fgb =
        FgbWriter::create_with_options(name, infer_flatgeobuf_geometry_type(table), options)
            .unwrap();
    table.process(&mut fgb).unwrap();
    fgb.write(writer).unwrap();
}

fn infer_flatgeobuf_geometry_type(table: &GeoTable) -> flatgeobuf::GeometryType {
    todo!()
}
