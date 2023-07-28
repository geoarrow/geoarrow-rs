use crate::error::GeoArrowError;
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::MultiPolygon;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<MultiPolygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPolygon<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a MultiPolygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a MultiPolygon<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        let num_polygons = value.num_polygons();
        let mut geos_geoms = Vec::with_capacity(num_polygons);

        for polygon_idx in 0..num_polygons {
            let polygon = value.polygon(polygon_idx).unwrap();
            geos_geoms.push(polygon.try_into()?);
        }

        Ok(geos::Geometry::create_multipolygon(geos_geoms)?)
    }
}
