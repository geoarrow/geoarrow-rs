use std::io::{Read, Seek};
use std::path::Path;
use std::sync::Arc;

use dbase::FieldType;
use shapefile::{Reader, ShapeReader};

use crate::array::PointBuilder;
use crate::table::Table;
use crate::trait_::NativeArray;

// TODO:
// stretch goal: return a record batch reader.
pub fn read_shapefile<T: Read + Seek>(shp_reader: T, dbf_reader: T) {
    let dbf_reader = dbase::Reader::new(dbf_reader).unwrap();
    let shp_reader = ShapeReader::new(shp_reader).unwrap();

    let header = shp_reader.header();
    // header.shape_type

    let fields = dbf_reader.fields();
    let field = &fields[0];
    // match field.field_type() {
    //     FieldType::
    // }

    let mut reader = Reader::new(shp_reader, dbf_reader);
    for x in reader.iter_shapes_and_records_as::<shapefile::Point, dbase::Record>() {
        let (geom, record) = x.unwrap();
        // let y = Point(&geom);
        PointBuilder::w
        // record is a wrapper around a hash map of values
    }
}

fn read_point<T: Read + Seek, D: Read + Seek>(reader: &mut Reader<T, D>) -> GeometryArrayRef {
    let mut builder = PointBuilder::<2>::with_capacity(reader.shape_count().unwrap());
    for row in reader.iter_shapes_and_records_as::<shapefile::Point, dbase::Record>() {
        let (geom, _record) = row.unwrap();
        builder.push_point(Some(&Point(&geom)));
    }
    Arc::new(builder.finish())
}
