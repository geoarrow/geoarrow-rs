use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::error::{WKBError, WKBResult};

/// The various WKB types supported by this crate
#[derive(Clone, Copy, Debug, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum WKBType {
    /// A WKB Point
    Point = 1,
    /// A WKB LineString
    LineString = 2,
    /// A WKB Polygon
    Polygon = 3,
    /// A WKB MultiPoint
    MultiPoint = 4,
    /// A WKB MultiLineString
    MultiLineString = 5,
    /// A WKB MultiPolygon
    MultiPolygon = 6,
    /// A WKB GeometryCollection
    GeometryCollection = 7,
    /// A WKB PointZ
    PointZ = 1001,
    /// A WKB LineStringZ
    LineStringZ = 1002,
    /// A WKB PolygonZ
    PolygonZ = 1003,
    /// A WKB MultiPointZ
    MultiPointZ = 1004,
    /// A WKB MultiLineStringZ
    MultiLineStringZ = 1005,
    /// A WKB MultiPolygonZ
    MultiPolygonZ = 1006,
    /// A WKB GeometryCollectionZ
    GeometryCollectionZ = 1007,
}

impl WKBType {
    /// Construct from a byte slice representing a WKB geometry
    pub fn from_buffer(buf: &[u8]) -> WKBResult<Self> {
        let mut reader = Cursor::new(buf);
        let byte_order = reader.read_u8().unwrap();
        let geometry_type = match byte_order {
            0 => reader.read_u32::<BigEndian>().unwrap(),
            1 => reader.read_u32::<LittleEndian>().unwrap(),
            _ => panic!("Unexpected byte order."),
        };
        Self::try_from_primitive(geometry_type).map_err(|err| WKBError::General(err.to_string()))
    }
}

// impl<'a, O: OffsetSizeTrait> TryFrom<WKB<'a, O>> for WKBType {
//     type Error = WKBError;

//     fn try_from(value: WKB<'a, O>) -> std::result::WKBResult<Self, Self::Error> {
//         Self::from_buffer(value.as_ref())
//     }
// }
