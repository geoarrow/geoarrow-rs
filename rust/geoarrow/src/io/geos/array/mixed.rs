use crate::array::{MixedGeometryArray, MixedGeometryBuilder};
use crate::datatypes::Dimension;
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSGeometry;

impl TryFrom<(Vec<geos::Geometry>, Dimension)> for MixedGeometryBuilder {
    type Error = GeoArrowError;

    fn try_from(
        (value, dim): (Vec<geos::Geometry>, Dimension),
    ) -> std::result::Result<Self, Self::Error> {
        let geoms: Vec<GEOSGeometry> = value.into_iter().map(GEOSGeometry::new).collect();
        Self::from_geometries(&geoms, dim, Default::default(), Default::default(), false)
    }
}

impl TryFrom<(Vec<geos::Geometry>, Dimension)> for MixedGeometryArray {
    type Error = GeoArrowError;

    fn try_from(value: (Vec<geos::Geometry>, Dimension)) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: MixedGeometryBuilder = value.try_into()?;
        Ok(mutable_arr.into())
    }
}
