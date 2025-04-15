use geo::Convert;
use geo_traits::to_geo::ToGeoPoint;
use geoarrow_array::array::PointArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Dimension, PointType};

use crate::raw;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn geoms() -> Vec<Option<geo_types::Point>> {
                raw::point::$mod_name::geoms()
                    .iter()
                    .map(|g| g.as_ref().map(|g| g.to_point().convert()))
                    .collect()
            }

            pub fn array(coord_type: CoordType) -> PointArray {
                let typ = PointType::new(coord_type, $dim, Default::default());
                PointBuilder::from_nullable_points(geoms().iter().map(|x| x.as_ref()), typ).finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
