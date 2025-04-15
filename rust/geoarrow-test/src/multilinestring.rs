use geo::Convert;
use geo_traits::to_geo::ToGeoMultiLineString;
use geoarrow_array::array::MultiLineStringArray;
use geoarrow_array::builder::MultiLineStringBuilder;
use geoarrow_schema::{CoordType, Dimension, MultiLineStringType};

use crate::raw;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn geoms() -> Vec<Option<geo_types::MultiLineString>> {
                raw::multilinestring::$mod_name::geoms()
                    .iter()
                    .map(|g| g.as_ref().map(|g| g.to_multi_line_string().convert()))
                    .collect()
            }

            pub fn array(coord_type: CoordType) -> MultiLineStringArray {
                let typ = MultiLineStringType::new(coord_type, $dim, Default::default());
                MultiLineStringBuilder::from_nullable_multi_line_strings(&geoms(), typ).finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
