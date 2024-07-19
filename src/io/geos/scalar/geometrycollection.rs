use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::GeometryCollection;
use arrow_array::OffsetSizeTrait;

impl<O: OffsetSizeTrait> TryFrom<GeometryCollection<'_, O, 2>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(
        value: GeometryCollection<'_, O, 2>,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<&'a GeometryCollection<'_, O, 2>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(
        value: &'a GeometryCollection<'_, O, 2>,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::create_geometry_collection(
            value
                .geometries()
                .map(|geometry| geometry.try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
}
