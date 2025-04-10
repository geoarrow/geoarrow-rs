use geo_types::{Point, point};

use crate::array::PointArray;
use crate::builder::PointBuilder;
use geo_traits::CoordTrait;
use geoarrow_schema::{CoordType, Dimension, PointType};

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
    let typ = PointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
    PointBuilder::from_points(geoms.iter(), typ).finish()
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
    let typ = PointType::new(CoordType::Interleaved, Dimension::XYZ, Default::default());
    let mut builder = PointBuilder::with_capacity(typ, 3);
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
