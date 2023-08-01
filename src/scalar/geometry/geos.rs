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
        use Geometry::*;

        match value {
            Point(g) => g.try_into(),
            LineString(g) => g.try_into(),
            Polygon(g) => g.try_into(),
            MultiPoint(g) => g.try_into(),
            MultiLineString(g) => g.try_into(),
            MultiPolygon(g) => g.try_into(),
            GeometryCollection(g) => g.try_into(),
            WKB(g) => g.try_into(),
            Rect(_g) => todo!(),
        }
    }
}
