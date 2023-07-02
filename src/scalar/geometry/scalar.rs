pub enum Geometry<'a> {
    Point(crate::scalar::Point<'a>),
    LineString(crate::scalar::LineString<'a>),
    Polygon(crate::scalar::Polygon<'a>),
    MultiPoint(crate::scalar::MultiPoint<'a>),
    MultiLineString(crate::scalar::MultiLineString<'a>),
    MultiPolygon(crate::scalar::MultiPolygon<'a>),
    WKB(crate::scalar::WKB<'a>),
}
