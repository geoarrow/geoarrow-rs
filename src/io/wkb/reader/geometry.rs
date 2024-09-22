use std::io::Cursor;

use arrow_array::OffsetSizeTrait;
use byteorder::ReadBytesExt;

use crate::datatypes::Dimension;
use crate::error::Result;
use crate::geo_traits::GeometryTrait;
use crate::io::wkb::common::WKBType;
use crate::io::wkb::reader::geometry_collection::WKBGeometryCollection;
use crate::io::wkb::reader::rect::WKBRect;
use crate::io::wkb::reader::{
    WKBLineString, WKBMaybeMultiLineString, WKBMaybeMultiPoint, WKBMaybeMultiPolygon,
    WKBMultiLineString, WKBMultiPoint, WKBMultiPolygon, WKBPoint, WKBPolygon,
};
use crate::scalar::WKB;

impl<'a, O: OffsetSizeTrait> WKB<'a, O> {
    /// Convert this WKB scalar to a [WKBGeometry]
    ///
    /// This "prepares" the WKB input for constant-time coordinate access.
    pub fn to_wkb_object(&'a self) -> WKBGeometry<'a> {
        let buf = self.as_slice();
        let mut reader = Cursor::new(buf);
        let byte_order = reader.read_u8().unwrap();
        let wkb_type = self.wkb_type().unwrap();

        use Dimension::*;

        match wkb_type {
            WKBType::Point => WKBGeometry::Point(WKBPoint::new(buf, byte_order.into(), 0, XY)),
            WKBType::LineString => {
                WKBGeometry::LineString(WKBLineString::new(buf, byte_order.into(), 0, XY))
            }
            WKBType::Polygon => {
                WKBGeometry::Polygon(WKBPolygon::new(buf, byte_order.into(), 0, XY))
            }
            WKBType::MultiPoint => {
                WKBGeometry::MultiPoint(WKBMultiPoint::new(buf, byte_order.into(), XY))
            }
            WKBType::MultiLineString => {
                WKBGeometry::MultiLineString(WKBMultiLineString::new(buf, byte_order.into(), XY))
            }
            WKBType::MultiPolygon => {
                WKBGeometry::MultiPolygon(WKBMultiPolygon::new(buf, byte_order.into(), XY))
            }
            WKBType::GeometryCollection => WKBGeometry::GeometryCollection(
                WKBGeometryCollection::new(buf, byte_order.into(), XY),
            ),
            WKBType::PointZ => WKBGeometry::Point(WKBPoint::new(buf, byte_order.into(), 0, XYZ)),
            WKBType::LineStringZ => {
                WKBGeometry::LineString(WKBLineString::new(buf, byte_order.into(), 0, XYZ))
            }
            WKBType::PolygonZ => {
                WKBGeometry::Polygon(WKBPolygon::new(buf, byte_order.into(), 0, XYZ))
            }
            WKBType::MultiPointZ => {
                WKBGeometry::MultiPoint(WKBMultiPoint::new(buf, byte_order.into(), XYZ))
            }
            WKBType::MultiLineStringZ => {
                WKBGeometry::MultiLineString(WKBMultiLineString::new(buf, byte_order.into(), XYZ))
            }
            WKBType::MultiPolygonZ => {
                WKBGeometry::MultiPolygon(WKBMultiPolygon::new(buf, byte_order.into(), XYZ))
            }
            WKBType::GeometryCollectionZ => WKBGeometry::GeometryCollection(
                WKBGeometryCollection::new(buf, byte_order.into(), XYZ),
            ),
        }
    }

    /// Access the [WKBType] of this WKB object.
    pub fn wkb_type(&'a self) -> Result<WKBType> {
        WKBType::from_buffer(self.as_ref())
    }
}

/// Endianness
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

    pub fn dimension(&self) -> Dimension {
        use WKBGeometry::*;
        match self {
            Point(g) => g.dimension(),
            LineString(g) => g.dimension(),
            Polygon(g) => g.dimension(),
            MultiPoint(g) => g.dimension(),
            MultiLineString(g) => g.dimension(),
            MultiPolygon(g) => g.dimension(),
            GeometryCollection(g) => g.dimension(),
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

    fn dim(&self) -> usize {
        self.dimension().size()
    }

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
