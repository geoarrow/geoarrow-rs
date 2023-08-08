use crate::error::{GeoArrowError, Result};
use crate::scalar::Geometry;
use arrow2::types::Offset;

impl<'b, O: Offset> TryFrom<Geometry<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: Geometry<'_, O>) -> Result<geos::Geometry<'b>> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, 'b, O: Offset> TryFrom<&'a Geometry<'_, O>> for geos::Geometry<'b> {
    type Error = GeoArrowError;

    fn try_from(value: &'a Geometry<'_, O>) -> Result<geos::Geometry<'b>> {
        match value {
            Geometry::Point(g) => g.try_into(),
            Geometry::LineString(g) => g.try_into(),
            Geometry::Polygon(g) => g.try_into(),
            Geometry::MultiPoint(g) => g.try_into(),
            Geometry::MultiLineString(g) => g.try_into(),
            Geometry::MultiPolygon(g) => g.try_into(),
            Geometry::WKB(g) => g.try_into(),
            Geometry::Rect(_g) => todo!(),
        }
    }
}
