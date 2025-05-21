use geo_types::{MultiPoint, point};
use geoarrow_schema::{CoordType, Dimension, MultiPointType};
use geoarrow_test::raw;

use crate::array::MultiPointArray;
use crate::builder::MultiPointBuilder;

pub(crate) fn mp0() -> MultiPoint {
    MultiPoint::new(vec![
        point!(
            x: 0., y: 1.
        ),
        point!(
            x: 1., y: 2.
        ),
    ])
}

pub(crate) fn mp1() -> MultiPoint {
    MultiPoint::new(vec![
        point!(
            x: 3., y: 4.
        ),
        point!(
            x: 5., y: 6.
        ),
    ])
}

pub(crate) fn mp_array(coord_type: CoordType) -> MultiPointArray {
    let geoms = vec![Some(mp0()), None, Some(mp1()), None];
    let typ = MultiPointType::new(coord_type, Dimension::XY, Default::default());
    MultiPointBuilder::from_nullable_multi_points(&geoms, typ).finish()
}

pub fn array(coord_type: CoordType, dim: Dimension) -> MultiPointArray {
    let typ = MultiPointType::new(coord_type, dim, Default::default());
    let geoms = match dim {
        Dimension::XY => raw::multipoint::xy::geoms(),
        Dimension::XYZ => raw::multipoint::xyz::geoms(),
        Dimension::XYM => raw::multipoint::xym::geoms(),
        Dimension::XYZM => raw::multipoint::xyzm::geoms(),
    };
    MultiPointBuilder::from_nullable_multi_points(&geoms, typ).finish()
}
