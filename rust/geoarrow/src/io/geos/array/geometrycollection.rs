use crate::array::{GeometryCollectionArray, GeometryCollectionBuilder};
use geoarrow_schema::Dimension;
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSGeometryCollection;

impl TryFrom<(Vec<geos::Geometry>, Dimension)> for GeometryCollectionBuilder {
    type Error = GeoArrowError;

    fn try_from(
        (value, dim): (Vec<geos::Geometry>, Dimension),
    ) -> std::result::Result<Self, Self::Error> {
        let geoms: Vec<GEOSGeometryCollection> = value
            .into_iter()
            .map(GEOSGeometryCollection::new_unchecked)
            .collect();
        Self::from_geometry_collections(&geoms, dim, Default::default(), Default::default(), false)
    }
}

impl TryFrom<(Vec<geos::Geometry>, Dimension)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from(value: (Vec<geos::Geometry>, Dimension)) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: GeometryCollectionBuilder = value.try_into()?;
        Ok(mutable_arr.into())
    }
}
