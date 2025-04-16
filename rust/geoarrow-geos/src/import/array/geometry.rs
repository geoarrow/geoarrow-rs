use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::GeometryType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSGeometry;

const DEFAULT_PREFER_MULTI: bool = false;

impl FromGEOS for GeometryBuilder {
    type GeoArrowType = GeometryType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSGeometry::new))
            .collect::<Vec<_>>();
        Self::from_nullable_geometries(&geoms, typ, DEFAULT_PREFER_MULTI)
    }
}

impl FromGEOS for GeometryArray {
    type GeoArrowType = GeometryType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(GeometryBuilder::from_geos(geoms, typ)?.finish())
    }
}
