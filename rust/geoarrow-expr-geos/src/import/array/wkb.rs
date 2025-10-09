use arrow_array::OffsetSizeTrait;
use geoarrow_array::array::GenericWkbArray;
use geoarrow_array::builder::WkbBuilder;
use geoarrow_schema::WkbType;
use geoarrow_schema::error::GeoArrowResult;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSGeometry;

impl<O: OffsetSizeTrait> FromGEOS for WkbBuilder<O> {
    type GeoArrowType = WkbType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSGeometry::new))
            .collect::<Vec<_>>();
        Self::from_nullable_geometries(&geoms, typ)
    }
}

impl<O: OffsetSizeTrait> FromGEOS for GenericWkbArray<O> {
    type GeoArrowType = WkbType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        Ok(WkbBuilder::from_geos(geoms, typ)?.finish())
    }
}
