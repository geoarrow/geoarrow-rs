use geo_types::{MultiPolygon, polygon};
use geoarrow_schema::{CoordType, Dimension, MultiPolygonType};
use geoarrow_test::raw;

use crate::array::MultiPolygonArray;
use crate::builder::MultiPolygonBuilder;

pub(crate) fn mp0() -> MultiPolygon {
    MultiPolygon::new(vec![
        polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ],
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
        ),
    ])
}

pub(crate) fn mp1() -> MultiPolygon {
    MultiPolygon::new(vec![
        polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ],
        polygon![
            (x: -110., y: 44.),
            (x: -110., y: 42.),
            (x: -105., y: 42.),
            (x: -105., y: 44.),
        ],
    ])
}

pub(crate) fn mp_array(coord_type: CoordType) -> MultiPolygonArray {
    let geoms = vec![Some(mp0()), None, Some(mp1()), None];
    let typ = MultiPolygonType::new(coord_type, Dimension::XY, Default::default());
    MultiPolygonBuilder::from_nullable_multi_polygons(&geoms, typ).finish()
}

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn array(coord_type: CoordType) -> MultiPolygonArray {
                let typ = MultiPolygonType::new(coord_type, $dim, Default::default());
                MultiPolygonBuilder::from_nullable_multi_polygons(
                    &raw::multipolygon::$mod_name::geoms(),
                    typ,
                )
                .finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
