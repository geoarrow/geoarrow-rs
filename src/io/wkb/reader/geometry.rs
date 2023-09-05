use std::io::Cursor;

use arrow2::types::Offset;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::GeometryTrait;
use crate::io::wkb::reader::geometry_collection::WKBGeometryCollection;
use crate::io::wkb::reader::rect::WKBRect;
use crate::io::wkb::reader::{
    WKBGeometryType, WKBLineString, WKBMaybeMultiLineString, WKBMaybeMultiPoint,
    WKBMaybeMultiPolygon, WKBMultiLineString, WKBMultiPoint, WKBMultiPolygon, WKBPoint, WKBPolygon,
};
use crate::scalar::WKB;

impl<'a, O: Offset> WKB<'a, O> {
    pub fn to_wkb_object(&'a self) -> WKBGeometry<'a, WKB<'a, O>> {
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

    pub fn into_wkb_object(self) -> WKBGeometry<'a, WKB<'a, O>> {
        let buf = self.arr.value(self.geom_index);
        let mut reader = Cursor::new(buf);
        let byte_order = reader.read_u8().unwrap();
        let geometry_type = match byte_order {
            0 => reader.read_u32::<BigEndian>().unwrap(),
            1 => reader.read_u32::<LittleEndian>().unwrap(),
            _ => panic!("Unexpected byte order."),
        };

        match geometry_type {
            1 => WKBGeometry::Point(WKBPoint::new(self, byte_order.into(), 0)),
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

    // pub fn to_wkb_line_string(&'a self) -> WKBLineString<'a> {
    //     match self.to_wkb_object() {
    //         WKBGeometry::LineString(geom) => geom,
    //         _ => panic!(),
    //     }
    // }

    // // pub fn to_wkb_maybe_multi_polygon(&'a self) ->
    // pub fn to_wkb_maybe_multi_polygon(&'a self) -> WKBMaybeMultiPolygon<'a> {
    //     match self.to_wkb_object() {
    //         WKBGeometry::Polygon(geom) => WKBMaybeMultiPolygon::Polygon(geom),
    //         WKBGeometry::MultiPolygon(geom) => WKBMaybeMultiPolygon::MultiPolygon(geom),
    //         _ => panic!(),
    //     }
    // }
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
pub enum WKBGeometry<'a, B: AsRef<[u8]> + 'a> {
    Point(WKBPoint<'a, B>),
    LineString(WKBLineString<'a, B>),
    Polygon(WKBPolygon<'a, B>),
    MultiPoint(WKBMultiPoint<'a, B>),
    MultiLineString(WKBMultiLineString<'a, B>),
    MultiPolygon(WKBMultiPolygon<'a, B>),
    GeometryCollection(WKBGeometryCollection<'a, B>),
}

impl<'a, B: AsRef<[u8]> + 'a> WKBGeometry<'a, B> {
    pub fn into_point(self) -> WKBPoint<'a, B> {
        match self {
            WKBGeometry::Point(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_line_string(self) -> WKBLineString<'a, B> {
        match self {
            WKBGeometry::LineString(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_polygon(self) -> WKBPolygon<'a, B> {
        match self {
            WKBGeometry::Polygon(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_multi_point(self) -> WKBMultiPoint<'a, B> {
        match self {
            WKBGeometry::MultiPoint(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_multi_line_string(self) -> WKBMultiLineString<'a, B> {
        match self {
            WKBGeometry::MultiLineString(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_multi_polygon(self) -> WKBMultiPolygon<'a, B> {
        match self {
            WKBGeometry::MultiPolygon(geom) => geom,
            _ => panic!(),
        }
    }

    pub fn into_maybe_multi_point(self) -> WKBMaybeMultiPoint<'a, B> {
        match self {
            WKBGeometry::Point(geom) => WKBMaybeMultiPoint::Point(geom),
            WKBGeometry::MultiPoint(geom) => WKBMaybeMultiPoint::MultiPoint(geom),
            _ => panic!(),
        }
    }

    pub fn into_maybe_multi_line_string(self) -> WKBMaybeMultiLineString<'a, B> {
        match self {
            WKBGeometry::LineString(geom) => WKBMaybeMultiLineString::LineString(geom),
            WKBGeometry::MultiLineString(geom) => WKBMaybeMultiLineString::MultiLineString(geom),
            _ => panic!(),
        }
    }
    pub fn into_maybe_multi_polygon(self) -> WKBMaybeMultiPolygon<'a, B> {
        match self {
            WKBGeometry::Polygon(geom) => WKBMaybeMultiPolygon::Polygon(geom),
            WKBGeometry::MultiPolygon(geom) => WKBMaybeMultiPolygon::MultiPolygon(geom),
            _ => panic!(),
        }
    }
}

impl<'a, B: AsRef<[u8]> + 'a> GeometryTrait<'a> for WKBGeometry<'a, B> {
    type T = f64;
    type Point = WKBPoint<'a, B>;
    type LineString = WKBLineString<'a, B>;
    type Polygon = WKBPolygon<'a, B>;
    type MultiPoint = WKBMultiPoint<'a, B>;
    type MultiLineString = WKBMultiLineString<'a, B>;
    type MultiPolygon = WKBMultiPolygon<'a, B>;
    type GeometryCollection = WKBGeometryCollection<'a, B>;
    type Rect = WKBRect<'a, B>;

    fn as_type(
        &'a self,
    ) -> crate::geo_traits::GeometryType<
        'a,
        WKBPoint<'a, B>,
        WKBLineString<'a, B>,
        WKBPolygon<'a, B>,
        WKBMultiPoint<'a, B>,
        WKBMultiLineString<'a, B>,
        WKBMultiPolygon<'a, B>,
        WKBGeometryCollection<'a, B>,
        WKBRect<'a, B>,
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
