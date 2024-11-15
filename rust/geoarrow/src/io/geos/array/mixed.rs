use crate::array::{MixedGeometryArray, MixedGeometryBuilder};
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSGeometry;

impl TryFrom<Vec<geos::Geometry>> for MixedGeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geos::Geometry>) -> std::result::Result<Self, Self::Error> {
        let geoms: Vec<GEOSGeometry> = value.into_iter().map(GEOSGeometry::new).collect();
        Self::from_geometries(&geoms, Default::default(), Default::default(), false)
    }
}

impl TryFrom<Vec<geos::Geometry>> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geos::Geometry>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: MixedGeometryBuilder = value.try_into()?;
        Ok(mutable_arr.into())
    }
}
