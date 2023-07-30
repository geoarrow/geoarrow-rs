use arrow2::types::Offset;

use crate::trait_::GeometryScalarTrait;

#[derive(Debug, PartialEq)]
pub enum Geometry<'a, O: Offset> {
    Point(crate::scalar::Point<'a>),
    LineString(crate::scalar::LineString<'a, O>),
    Polygon(crate::scalar::Polygon<'a, O>),
    MultiPoint(crate::scalar::MultiPoint<'a, O>),
    MultiLineString(crate::scalar::MultiLineString<'a, O>),
    MultiPolygon(crate::scalar::MultiPolygon<'a, O>),
    WKB(crate::scalar::WKB<'a, O>),
    Rect(crate::scalar::Rect<'a>),
}

impl<'a, O: Offset> GeometryScalarTrait<'a> for Geometry<'a, O> {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        match self {
            Geometry::Point(g) => geo::Geometry::Point(g.into()),
            Geometry::LineString(g) => geo::Geometry::LineString(g.into()),
            Geometry::Polygon(g) => geo::Geometry::Polygon(g.into()),
            Geometry::MultiPoint(g) => geo::Geometry::MultiPoint(g.into()),
            Geometry::MultiLineString(g) => geo::Geometry::MultiLineString(g.into()),
            Geometry::MultiPolygon(g) => geo::Geometry::MultiPolygon(g.into()),
            Geometry::WKB(g) => g.into(),
            Geometry::Rect(g) => geo::Geometry::Rect(g.into()),
        }
    }
}
