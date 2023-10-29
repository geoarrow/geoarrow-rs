use crate::error::{GeoArrowError, Result};
use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::GeometryCollection;
use arrow_array::OffsetSizeTrait;

impl<'b, O: OffsetSizeTrait> TryFrom<GeometryCollection<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollection<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: OffsetSizeTrait> TryFrom<&'a GeometryCollection<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a GeometryCollection<'_, O>) -> Result<geos::Geometry<'b>> {
        let num_geometries = value.num_geometries();
        let mut geoms: Vec<geos::Geometry<'_>> = Vec::with_capacity(num_geometries);
        for i in 0..num_geometries {
            let geom = value.geometry(i).unwrap();
            geoms.push(geom.try_into()?)
        }

        Ok(geos::Geometry::create_geometry_collection(geoms)?)
    }
}
