use geo::wkt;
use geo_types::{Point, point};

use crate::array::PointArray;
use crate::builder::PointBuilder;
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

pub(crate) fn p3() -> Point {
    wkt! {
        POINT (30.0 10.0)
    }
}

pub(crate) fn point_array(coord_type: CoordType) -> PointArray {
    let geoms = [Some(p0()), Some(p1()), None, Some(p2())];
    let typ = PointType::new(coord_type, Dimension::XY, Default::default());
    PointBuilder::from_nullable_points(geoms.iter().map(|x| x.as_ref()), typ).finish()
}
