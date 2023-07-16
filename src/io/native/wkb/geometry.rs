use std::io::Cursor;

use arrow2::types::Offset;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::io::native::wkb::linestring::WKBLineString;
use crate::io::native::wkb::maybe_multi_line_string::WKBMaybeMultiLineString;
use crate::io::native::wkb::maybe_multi_point::WKBMaybeMultiPoint;
use crate::io::native::wkb::maybe_multipolygon::WKBMaybeMultiPolygon;
use crate::io::native::wkb::multilinestring::WKBMultiLineString;
use crate::io::native::wkb::multipoint::WKBMultiPoint;
use crate::io::native::wkb::multipolygon::WKBMultiPolygon;
use crate::io::native::wkb::point::WKBPoint;
use crate::io::native::wkb::polygon::WKBPolygon;
use crate::scalar::WKB;

impl<'a, O: Offset> WKB<'a, O> {
    pub fn to_wkb_object(&'a self) -> WKBGeometry<'a> {
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

    pub fn to_wkb_line_string(&'a self) -> WKBLineString<'a> {
        match self.to_wkb_object() {
            WKBGeometry::LineString(geom) => geom,
            _ => panic!(),
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

impl<'a> WKBGeometry<'a> {
    pub fn to_point(self) -> WKBPoint<'a> {
        match self {
            WKBGeometry::Point(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn to_line_string(self) -> WKBLineString<'a> {
        match self {
            WKBGeometry::LineString(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn to_polygon(self) -> WKBPolygon<'a> {
        match self {
            WKBGeometry::Polygon(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn to_multi_point(self) -> WKBMultiPoint<'a> {
        match self {
            WKBGeometry::MultiPoint(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn to_multi_line_string(self) -> WKBMultiLineString<'a> {
        match self {
            WKBGeometry::MultiLineString(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn to_multi_polygon(self) -> WKBMultiPolygon<'a> {
        match self {
            WKBGeometry::MultiPolygon(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn to_maybe_multi_point(self) -> WKBMaybeMultiPoint<'a> {
        match self {
            WKBGeometry::Point(geom) => WKBMaybeMultiPoint::Point(geom),
            WKBGeometry::MultiPoint(geom) => WKBMaybeMultiPoint::MultiPoint(geom),
            _ => panic!(),
        }
    }

    pub fn to_maybe_multi_line_string(self) -> WKBMaybeMultiLineString<'a> {
        match self {
            WKBGeometry::LineString(geom) => WKBMaybeMultiLineString::LineString(geom),
            WKBGeometry::MultiLineString(geom) => WKBMaybeMultiLineString::MultiLineString(geom),
            _ => panic!(),
        }
    }
    pub fn to_maybe_multi_polygon(self) -> WKBMaybeMultiPolygon<'a> {
        match self {
            WKBGeometry::Polygon(geom) => WKBMaybeMultiPolygon::Polygon(geom),
            WKBGeometry::MultiPolygon(geom) => WKBMaybeMultiPolygon::MultiPolygon(geom),
            _ => panic!(),
        }
    }
}

impl<'a> From<WKBGeometry<'a>> for WKBLineString<'a> {
    fn from(value: WKBGeometry<'a>) -> Self {
        match value {
            WKBGeometry::LineString(geom) => geom,
            _ => panic!(),
        }
    }
}
