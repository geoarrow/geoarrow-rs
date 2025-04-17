use core::panic;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use geo::{Point, point};

use crate::ArrayBase;
use crate::array::{PointArray, PointBuilder};
use crate::table::Table;
use crate::test::properties;
use geo_traits::CoordTrait;
use geoarrow_schema::{CoordType, Dimension};

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
    let geoms = [p0(), p1(), p2()];
    PointBuilder::from_points(
        geoms.iter(),
        Dimension::XY,
        CoordType::default_interleaved(),
        Default::default(),
    )
    .finish()
}

struct CoordZ {
    x: f64,
    y: f64,
    z: f64,
}

impl CoordTrait for CoordZ {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xyz
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
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

pub(crate) fn point_z_array() -> PointArray {
    let mut builder = PointBuilder::with_capacity(Dimension::XYZ, 3);
    let coords = vec![
        CoordZ {
            x: 0.,
            y: 1.,
            z: 2.,
        },
        CoordZ {
            x: 3.,
            y: 4.,
            z: 5.,
        },
        CoordZ {
            x: 6.,
            y: 7.,
            z: 8.,
        },
    ];
    for coord in &coords {
        builder.push_coord(Some(coord));
    }
    builder.finish()
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
