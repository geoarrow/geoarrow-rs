use geo::Convert;
use geo_traits::to_geo::ToGeoMultiPoint;
use geoarrow_array::array::MultiPointArray;
use geoarrow_array::builder::MultiPointBuilder;
use geoarrow_schema::{CoordType, Dimension, MultiPointType};

use crate::raw;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn geoms() -> Vec<Option<geo_types::MultiPoint>> {
                raw::multipoint::$mod_name::geoms()
                    .iter()
                    .map(|g| g.as_ref().map(|g| g.to_multi_point().convert()))
                    .collect()
            }

            pub fn array(coord_type: CoordType) -> MultiPointArray {
                let typ = MultiPointType::new(coord_type, $dim, Default::default());
                MultiPointBuilder::from_nullable_multi_points(&geoms(), typ).finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
