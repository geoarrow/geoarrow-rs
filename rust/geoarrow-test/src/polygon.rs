use geo::Convert;
use geo_traits::to_geo::ToGeoPolygon;
use geoarrow_array::array::PolygonArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_schema::{CoordType, Dimension, PolygonType};

use crate::raw;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn geoms() -> Vec<Option<geo_types::Polygon>> {
                raw::polygon::$mod_name::geoms()
                    .iter()
                    .map(|g| g.as_ref().map(|g| g.to_polygon().convert()))
                    .collect()
            }

            pub fn array(coord_type: CoordType) -> PolygonArray {
                let typ = PolygonType::new(coord_type, $dim, Default::default());
                PolygonBuilder::from_nullable_polygons(&geoms(), typ).finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
