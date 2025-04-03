use geo::{point, MultiPoint};

use crate::array::{MultiPointArray, MultiPointBuilder};
use geoarrow_schema::{CoordType, Dimension};

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

pub(crate) fn mp_array() -> MultiPointArray {
    let geoms = vec![mp0(), mp1()];
    MultiPointBuilder::from_multi_points(
        &geoms,
        Dimension::XY,
        CoordType::Interleaved,
        Default::default(),
    )
    .finish()
}
