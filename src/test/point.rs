use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
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
    vec![p0(), p1(), p2()].into()
}

pub(crate) fn table() -> GeoTable {
    let point_array: PointArray = vec![p0(), p1(), p2()].into();
    let u8_array = properties::u8_array();
    let string_array = properties::string_array();

    let fields = vec![
        Field::new("u8", DataType::UInt8, true),
        Field::new("string", DataType::Utf8, true),
        Field::new("geometry", point_array.extension_type(), true),
    ];
    let schema: Schema = fields.into();

    let chunk = Chunk::new(vec![
        u8_array.boxed(),
        string_array.boxed(),
        point_array.into_boxed_arrow(),
    ]);

    GeoTable::try_new(schema, vec![chunk], 2).unwrap()
}
