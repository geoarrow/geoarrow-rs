use geoarrow_schema::{CoordType, GeometryType};
use geoarrow_test::raw;

use crate::array::GeometryArray;
use crate::builder::GeometryBuilder;

pub fn array(coord_type: CoordType, _prefer_multi: bool) -> GeometryArray {
    let typ = GeometryType::new(coord_type, Default::default());
    GeometryBuilder::from_nullable_geometries(&raw::geometry::geoms(), typ)
        .unwrap()
        .finish()
}
