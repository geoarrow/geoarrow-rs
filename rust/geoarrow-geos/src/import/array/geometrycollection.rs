use geoarrow_array::array::GeometryCollectionArray;
use geoarrow_array::builder::GeometryCollectionBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::GeometryCollectionType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSGeometryCollection;

const DEFAULT_PREFER_MULTI: bool = false;

impl FromGEOS for GeometryCollectionBuilder {
    type GeoArrowType = GeometryCollectionType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSGeometryCollection::try_new).transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometry_collections(&geoms, typ, DEFAULT_PREFER_MULTI)
    }
}

impl FromGEOS for GeometryCollectionArray {
    type GeoArrowType = GeometryCollectionType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(GeometryCollectionBuilder::from_geos(geoms, typ)?.finish())
    }
}
