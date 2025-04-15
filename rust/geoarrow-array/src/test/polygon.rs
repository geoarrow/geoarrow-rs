use geo_types::{Polygon, polygon};
use geoarrow_schema::{CoordType, Dimension, PolygonType};
use geoarrow_test::raw;

use crate::array::PolygonArray;
use crate::builder::PolygonBuilder;

pub(crate) fn p0() -> Polygon {
    polygon![
        (x: -111., y: 45.),
        (x: -111., y: 41.),
        (x: -104., y: 41.),
        (x: -104., y: 45.),
    ]
}

pub(crate) fn p1() -> Polygon {
    polygon!(
        exterior: [
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ],
        interiors: [
            [
                (x: -110., y: 44.),
                (x: -110., y: 42.),
                (x: -105., y: 42.),
                (x: -105., y: 44.),
            ],
        ],
    )
}

pub(crate) fn p_array(coord_type: CoordType) -> PolygonArray {
    let geoms = vec![Some(p0()), None, Some(p1()), None];
    let typ = PolygonType::new(coord_type, Dimension::XY, Default::default());
    PolygonBuilder::from_nullable_polygons(&geoms, typ).finish()
}

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn array(coord_type: CoordType) -> PolygonArray {
                let typ = PolygonType::new(coord_type, $dim, Default::default());
                PolygonBuilder::from_nullable_polygons(&raw::polygon::$mod_name::geoms(), typ)
                    .finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
