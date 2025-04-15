use geoarrow_schema::{CoordType, Dimension, GeometryCollectionType};
use geoarrow_test::raw;

use crate::array::GeometryCollectionArray;
use crate::builder::GeometryCollectionBuilder;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn array(coord_type: CoordType, prefer_multi: bool) -> GeometryCollectionArray {
                let typ = GeometryCollectionType::new(coord_type, $dim, Default::default());
                GeometryCollectionBuilder::from_nullable_geometry_collections(
                    &raw::geometrycollection::$mod_name::geoms(),
                    typ,
                    prefer_multi,
                )
                .unwrap()
                .finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
