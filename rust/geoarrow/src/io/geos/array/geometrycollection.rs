use crate::array::{GeometryCollectionArray, GeometryCollectionBuilder};
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSGeometryCollection;

impl TryFrom<Vec<geos::Geometry>> for GeometryCollectionBuilder<D> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geos::Geometry>) -> std::result::Result<Self, Self::Error> {
        let geoms: Vec<GEOSGeometryCollection> = value
            .into_iter()
            .map(GEOSGeometryCollection::new_unchecked)
            .collect();
        Self::from_geometry_collections(&geoms, Default::default(), Default::default(), false)
    }
}

impl TryFrom<Vec<geos::Geometry>> for GeometryCollectionArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<geos::Geometry>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: GeometryCollectionBuilder<D> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}
