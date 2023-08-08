use crate::error::GeoArrowError;
use crate::geo_traits::PolygonTrait;
use crate::scalar::Polygon;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<Polygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: Polygon<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a Polygon<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a Polygon<'_, O>) -> Result<geos::Geometry<'b>, Self::Error> {
        if let Some(exterior) = value.exterior() {
            let exterior = exterior.to_geos_linear_ring()?;
            let num_interiors = value.num_interiors();

            let mut interiors = Vec::with_capacity(num_interiors);

            for interior_idx in 0..num_interiors {
                let interior = value.interior(interior_idx).unwrap();
                interiors.push(interior.to_geos_linear_ring()?);
            }

            Ok(geos::Geometry::create_polygon(exterior, interiors)?)
        } else {
            Ok(geos::Geometry::create_empty_polygon()?)
        }
    }
}
