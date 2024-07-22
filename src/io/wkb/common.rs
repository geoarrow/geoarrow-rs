use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum WKBType {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
    PointZ = 1001,
    LineStringZ = 1002,
    PolygonZ = 1003,
    MultiPointZ = 1004,
    MultiLineStringZ = 1005,
    MultiPolygonZ = 1006,
    GeometryCollectionZ = 1007,
}
