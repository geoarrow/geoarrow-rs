use geoarrow_schema::{CoordType, Dimension, GeometryCollectionType};
use geoarrow_test::raw;

use crate::array::GeometryCollectionArray;
use crate::builder::GeometryCollectionBuilder;

pub fn array(
    coord_type: CoordType,
    dim: Dimension,
    _prefer_multi: bool,
) -> GeometryCollectionArray {
    let typ = GeometryCollectionType::new(dim).with_coord_type(coord_type);
    let geoms = match dim {
        Dimension::XY => raw::geometrycollection::xy::geoms(),
        Dimension::XYZ => raw::geometrycollection::xyz::geoms(),
        Dimension::XYM => raw::geometrycollection::xym::geoms(),
        Dimension::XYZM => raw::geometrycollection::xyzm::geoms(),
    };

    GeometryCollectionBuilder::from_nullable_geometry_collections(&geoms, typ)
        .unwrap()
        .finish()
}
