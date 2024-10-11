use core::panic;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use geo::{point, Point};

use crate::array::PointArray;
use crate::geo_traits::PointTrait;
use crate::table::Table;
use crate::test::properties;
use crate::ArrayBase;

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

pub(crate) fn point_array() -> PointArray<2> {
    vec![p0(), p1(), p2()].as_slice().into()
}

struct PointZ {
    x: f64,
    y: f64,
    z: f64,
}

impl PointTrait for PointZ {
    type T = f64;

    fn dim(&self) -> crate::geo_traits::Dimension {
        crate::geo_traits::Dimension::XYZ
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.x,
            1 => self.y,
            2 => self.z,
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.x
    }

    fn y(&self) -> Self::T {
        self.y
    }
}

pub(crate) fn point_z_array() -> PointArray<3> {
    vec![
        PointZ {
            x: 0.,
            y: 1.,
            z: 2.,
        },
        PointZ {
            x: 3.,
            y: 4.,
            z: 5.,
        },
        PointZ {
            x: 6.,
            y: 7.,
            z: 8.,
        },
    ]
    .as_slice()
    .into()
}

pub(crate) fn table() -> Table {
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

    Table::try_new(vec![batch], schema).unwrap()
}

pub(crate) fn table_z() -> Table {
    let point_array = point_z_array();
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

    Table::try_new(vec![batch], schema).unwrap()
}
