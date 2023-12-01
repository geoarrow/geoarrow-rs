use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use geo::{point, Point};

use crate::array::PointArray;
use crate::table::GeoTable;
use crate::test::properties;
use crate::GeometryArrayTrait;

pub(crate) fn p0() -> Point {
    point!(
        x: 0., y: 1.
    )
}

pub(crate) fn p1() -> Point {
    point!(
        x: 1., y: 2.
    )
}

pub(crate) fn p2() -> Point {
    point!(
        x: 2., y: 3.
    )
}

pub(crate) fn point_array() -> PointArray {
    vec![p0(), p1(), p2()].as_slice().into()
}

pub(crate) fn table() -> GeoTable {
    let point_array = point_array();
    let u8_array = properties::u8_array();
    let string_array = properties::string_array();

    let fields = vec![
        Arc::new(Field::new("u8", DataType::UInt8, true)),
        Arc::new(Field::new("string", DataType::Utf8, true)),
        point_array.extension_field(),
    ];
    let schema = Arc::new(Schema::new(fields));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(u8_array),
            Arc::new(string_array),
            point_array.into_array_ref(),
        ],
    )
    .unwrap();

    GeoTable::try_new(schema, vec![batch], 2).unwrap()
}
