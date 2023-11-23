#![allow(non_upper_case_globals)]

use num_enum::TryFromPrimitive;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u32)]
pub enum WKBGeometryType {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct WKBOptions {
    pub is_ewkb: bool,
    pub has_srid: bool,
}

impl WKBOptions {
    // Taken from wkx under the MIT license
    // https://github.com/cschwarz/wkx/blob/b85b7b93af101a87a7a0fffa2a6d68c551b3f8d3/lib/geometry.js#L73
    pub fn from_geometry_type(geometry_type_int: u32) -> Self {
        let has_srid = geometry_type_int & 0x20000000 == 0x20000000;
        let is_ewkb = (geometry_type_int & 0x20000000)
            | (geometry_type_int & 0x40000000)
            | (geometry_type_int & 0x80000000)
            != 0;

        Self { is_ewkb, has_srid }
    }
}
