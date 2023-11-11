use crate::array::CoordType;

#[derive(Debug, Clone, PartialEq)]
pub enum GeoDataType {
    Point(CoordType),
    LineString(CoordType),
    LargeLineString(CoordType),
    Polygon(CoordType),
    LargePolygon(CoordType),
    MultiPoint(CoordType),
    LargeMultiPoint(CoordType),
    MultiLineString(CoordType),
    LargeMultiLineString(CoordType),
    MultiPolygon(CoordType),
    LargeMultiPolygon(CoordType),
    Mixed(CoordType),
    LargeMixed(CoordType),
    GeometryCollection(CoordType),
    LargeGeometryCollection(CoordType),
    WKB,
    LargeWKB,
    Rect,
}
