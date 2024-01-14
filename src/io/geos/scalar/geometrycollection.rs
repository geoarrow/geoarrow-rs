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
        Ok(geos::Geometry::create_geometry_collection(
            value
                .geometries()
                .map(|geometry| geometry.try_into())
                .collect::<Result<Vec<_>>>()?,
        )?)
    }
}
