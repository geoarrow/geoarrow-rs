pub enum Geometry<'a> {
    Point(crate::Point<'a>),
    LineString(crate::LineString<'a>),
    Polygon(crate::Polygon<'a>),
    MultiPoint(crate::MultiPoint<'a>),
    MultiLineString(crate::MultiLineString<'a>),
    MultiPolygon(crate::MultiPolygon<'a>),
    WKB(crate::WKB<'a>),
}
