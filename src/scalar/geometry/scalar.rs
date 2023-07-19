use arrow2::types::Offset;

#[derive(Debug, Clone, PartialEq)]
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
