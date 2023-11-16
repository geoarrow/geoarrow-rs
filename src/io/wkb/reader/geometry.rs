use std::io::Cursor;

use arrow_array::OffsetSizeTrait;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::GeometryTrait;
use crate::io::wkb::reader::geometry_collection::WKBGeometryCollection;
use crate::io::wkb::reader::rect::WKBRect;
use crate::io::wkb::reader::{
    WKBGeometryType, WKBLineString, WKBMaybeMultiLineString, WKBMaybeMultiPoint,
    WKBMaybeMultiPolygon, WKBMultiLineString, WKBMultiPoint, WKBMultiPolygon, WKBPoint, WKBPolygon,
};
use crate::scalar::WKB;

impl<'a, O: OffsetSizeTrait> WKB<'a, O> {
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
            7 => {
                WKBGeometry::GeometryCollection(WKBGeometryCollection::new(buf, byte_order.into()))
            }
            _ => panic!("Unexpected geometry type"),
        }
    }

    pub fn get_wkb_geometry_type(&'a self) -> WKBGeometryType {
        let buf = self.arr.value(self.geom_index);
        let mut reader = Cursor::new(buf);
        let byte_order = reader.read_u8().unwrap();
        let geometry_type = match byte_order {
            0 => reader.read_u32::<BigEndian>().unwrap(),
            1 => reader.read_u32::<LittleEndian>().unwrap(),
            _ => panic!("Unexpected byte order."),
        };
        geometry_type.try_into().unwrap()
    }

    pub fn to_wkb_line_string(&'a self) -> WKBLineString<'a> {
        match self.to_wkb_object() {
            WKBGeometry::LineString(geom) => geom,
            _ => panic!(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
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

impl From<Endianness> for u8 {
    fn from(value: Endianness) -> Self {
        use Endianness::*;
        match value {
            BigEndian => 0,
            LittleEndian => 1,
        }
    }
}

#[derive(Debug, Clone)]
pub enum WKBGeometry<'a> {
    Point(WKBPoint<'a>),
    LineString(WKBLineString<'a>),
    Polygon(WKBPolygon<'a>),
    MultiPoint(WKBMultiPoint<'a>),
    MultiLineString(WKBMultiLineString<'a>),
    MultiPolygon(WKBMultiPolygon<'a>),
    GeometryCollection(WKBGeometryCollection<'a>),
}

impl<'a> WKBGeometry<'a> {
    pub fn into_point(self) -> WKBPoint<'a> {
        match self {
            WKBGeometry::Point(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_line_string(self) -> WKBLineString<'a> {
        match self {
            WKBGeometry::LineString(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_polygon(self) -> WKBPolygon<'a> {
        match self {
            WKBGeometry::Polygon(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_multi_point(self) -> WKBMultiPoint<'a> {
        match self {
            WKBGeometry::MultiPoint(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_multi_line_string(self) -> WKBMultiLineString<'a> {
        match self {
            WKBGeometry::MultiLineString(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_multi_polygon(self) -> WKBMultiPolygon<'a> {
        match self {
            WKBGeometry::MultiPolygon(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_maybe_multi_point(self) -> WKBMaybeMultiPoint<'a> {
        match self {
            WKBGeometry::Point(geom) => WKBMaybeMultiPoint::Point(geom),
            WKBGeometry::MultiPoint(geom) => WKBMaybeMultiPoint::MultiPoint(geom),
            _ => panic!(),
        }
    }

    pub fn into_maybe_multi_line_string(self) -> WKBMaybeMultiLineString<'a> {
        match self {
            WKBGeometry::LineString(geom) => WKBMaybeMultiLineString::LineString(geom),
            WKBGeometry::MultiLineString(geom) => WKBMaybeMultiLineString::MultiLineString(geom),
            _ => panic!(),
        }
    }
    pub fn into_maybe_multi_polygon(self) -> WKBMaybeMultiPolygon<'a> {
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

impl<'a> GeometryTrait for WKBGeometry<'a> {
    type T = f64;
    type Point<'b> = WKBPoint<'a> where Self: 'b;
    type LineString<'b> = WKBLineString<'a> where Self: 'b;
    type Polygon<'b> = WKBPolygon<'a> where Self: 'b;
    type MultiPoint<'b> = WKBMultiPoint<'a> where Self: 'b;
    type MultiLineString<'b> = WKBMultiLineString<'a> where Self: 'b;
    type MultiPolygon<'b> = WKBMultiPolygon<'a> where Self: 'b;
    type GeometryCollection<'b> = WKBGeometryCollection<'a> where Self: 'b;
    type Rect<'b> = WKBRect<'a> where Self: 'b;

    fn as_type(
        &self,
    ) -> crate::geo_traits::GeometryType<
        '_,
        WKBPoint<'a>,
        WKBLineString<'a>,
        WKBPolygon<'a>,
        WKBMultiPoint<'a>,
        WKBMultiLineString<'a>,
        WKBMultiPolygon<'a>,
        WKBGeometryCollection<'a>,
        WKBRect<'a>,
    > {
        use crate::geo_traits::GeometryType as B;
        use WKBGeometry as A;
        match self {
            A::Point(p) => B::Point(p),
            A::LineString(ls) => B::LineString(ls),
            A::Polygon(ls) => B::Polygon(ls),
            A::MultiPoint(ls) => B::MultiPoint(ls),
            A::MultiLineString(ls) => B::MultiLineString(ls),
            A::MultiPolygon(ls) => B::MultiPolygon(ls),
            A::GeometryCollection(gc) => B::GeometryCollection(gc),
        }
    }
}
