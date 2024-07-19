use crate::scalar::Geometry;
use arrow_array::OffsetSizeTrait;

impl<O: OffsetSizeTrait, const D: usize> TryFrom<Geometry<'_, O, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: Geometry<'_, O, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::try_from(&value)
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> TryFrom<&'a Geometry<'_, O, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a Geometry<'_, O, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        match value {
            Geometry::Point(g) => g.try_into(),
            Geometry::LineString(g) => g.try_into(),
            Geometry::Polygon(g) => g.try_into(),
            Geometry::MultiPoint(g) => g.try_into(),
            Geometry::MultiLineString(g) => g.try_into(),
            Geometry::MultiPolygon(g) => g.try_into(),
            Geometry::GeometryCollection(g) => g.try_into(),
            Geometry::Rect(_g) => todo!(),
        }
    }
}
