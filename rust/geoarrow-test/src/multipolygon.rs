use geo::Convert;
use geo_traits::to_geo::ToGeoMultiPolygon;
use geoarrow_array::array::MultiPolygonArray;
use geoarrow_array::builder::MultiPolygonBuilder;
use geoarrow_schema::{CoordType, Dimension, MultiPolygonType};

use crate::raw;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn geoms() -> Vec<Option<geo_types::MultiPolygon>> {
                raw::multipolygon::$mod_name::geoms()
                    .iter()
                    .map(|g| g.as_ref().map(|g| g.to_multi_polygon().convert()))
                    .collect()
            }

            pub fn array(coord_type: CoordType) -> MultiPolygonArray {
                let typ = MultiPolygonType::new(coord_type, $dim, Default::default());
                MultiPolygonBuilder::from_nullable_multi_polygons(&geoms(), typ).finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
