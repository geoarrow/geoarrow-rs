use crate::error::GeoArrowError;
use crate::geo_traits::MultiPointTrait;
use crate::scalar::MultiPoint;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<MultiPoint<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPoint<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a MultiPoint<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a MultiPoint<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        let num_points = value.num_points();
        let mut geos_geoms = Vec::with_capacity(num_points);

        for point_idx in 0..num_points {
            let point = value.point(point_idx).unwrap();
            geos_geoms.push(point.try_into()?);
        }

        Ok(geos::Geometry::create_multipoint(geos_geoms)?)
    }
}
