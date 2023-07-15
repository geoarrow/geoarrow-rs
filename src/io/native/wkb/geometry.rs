use std::io::Cursor;

use arrow2::types::Offset;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::io::native::wkb::linestring::WKBLineString;
use crate::io::native::wkb::multilinestring::WKBMultiLineString;
use crate::io::native::wkb::multipoint::WKBMultiPoint;
use crate::io::native::wkb::multipolygon::WKBMultiPolygon;
use crate::io::native::wkb::point::WKBPoint;
use crate::io::native::wkb::polygon::WKBPolygon;
use crate::scalar::WKB;

impl<'a, O: Offset> WKB<'a, O> {
    pub fn to_wkb_object(&self) -> WKBGeometry {
        let buf = self.arr.value(self.geom_index);
        let mut reader = Cursor::new(buf);
        let byte_order = reader.read_u8().unwrap();
        let geometry_type = match byte_order {
            0 => reader.read_u32::<BigEndian>().unwrap(),
            1 => reader.read_u32::<LittleEndian>().unwrap(),
            _ => panic!("Unexpected byte order."),
        };

        match geometry_type {
            1 => WKBGeometry::Point(WKBPoint::new(buf, byte_order.into(), 0)),
            2 => WKBGeometry::LineString(WKBLineString::new(buf, byte_order.into(), 0)),
            3 => WKBGeometry::Polygon(WKBPolygon::new(buf, byte_order.into(), 0)),
            4 => WKBGeometry::MultiPoint(WKBMultiPoint::new(buf, byte_order.into())),
            5 => WKBGeometry::MultiLineString(WKBMultiLineString::new(buf, byte_order.into())),
            6 => WKBGeometry::MultiPolygon(WKBMultiPolygon::new(buf, byte_order.into())),
            _ => panic!("Unexpected geometry type"),
        }
    }
}

#[derive(Clone, Copy)]
pub enum Endianness {
    BigEndian,
    LittleEndian,
}

impl From<u8> for Endianness {
    fn from(value: u8) -> Self {
        match value {
            0 => Endianness::BigEndian,
            1 => Endianness::LittleEndian,
            _ => panic!("Unexpected byte order."),
        }
    }
}

pub enum WKBGeometry<'a> {
    Point(WKBPoint<'a>),
    LineString(WKBLineString<'a>),
    Polygon(WKBPolygon<'a>),
    MultiPoint(WKBMultiPoint<'a>),
    MultiLineString(WKBMultiLineString<'a>),
    MultiPolygon(WKBMultiPolygon<'a>),
}
