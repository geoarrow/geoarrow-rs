use geo::Convert;
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_schema::{CoordType, GeometryType};

use crate::raw;

pub fn geoms() -> Vec<Option<geo_types::Geometry>> {
    raw::geometry::geoms()
        .iter()
        .map(|g| g.as_ref().map(|g| g.to_geometry().convert()))
        .collect()
}

pub fn array(coord_type: CoordType, prefer_multi: bool) -> GeometryArray {
    let typ = GeometryType::new(coord_type, Default::default());
    GeometryBuilder::from_nullable_geometries(&geoms(), typ, prefer_multi)
        .unwrap()
        .finish()
}
