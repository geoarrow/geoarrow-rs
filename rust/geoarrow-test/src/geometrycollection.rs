use geo::Convert;
use geo_traits::to_geo::ToGeoGeometryCollection;
use geoarrow_array::array::GeometryCollectionArray;
use geoarrow_array::builder::GeometryCollectionBuilder;
use geoarrow_schema::{CoordType, Dimension, GeometryCollectionType};

use crate::raw;

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn geoms() -> Vec<Option<geo_types::GeometryCollection>> {
                raw::geometrycollection::$mod_name::geoms()
                    .iter()
                    .map(|g| g.as_ref().map(|g| g.to_geometry_collection().convert()))
                    .collect()
            }

            pub fn array(coord_type: CoordType, prefer_multi: bool) -> GeometryCollectionArray {
                let typ = GeometryCollectionType::new(coord_type, $dim, Default::default());
                GeometryCollectionBuilder::from_nullable_geometry_collections(
                    &geoms(),
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
