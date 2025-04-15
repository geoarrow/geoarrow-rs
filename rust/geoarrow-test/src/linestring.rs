use geo::Convert;
use geo_traits::to_geo::ToGeoLineString;
use geoarrow_array::array::LineStringArray;
use geoarrow_array::builder::LineStringBuilder;
use geoarrow_schema::{CoordType, Dimension, LineStringType};

use crate::raw;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn geoms() -> Vec<Option<geo_types::LineString>> {
                raw::linestring::$mod_name::geoms()
                    .iter()
                    .map(|g| g.as_ref().map(|g| g.to_line_string().convert()))
                    .collect()
            }

            pub fn array(coord_type: CoordType) -> LineStringArray {
                let typ = LineStringType::new(coord_type, $dim, Default::default());
                LineStringBuilder::from_nullable_line_strings(&geoms(), typ).finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
