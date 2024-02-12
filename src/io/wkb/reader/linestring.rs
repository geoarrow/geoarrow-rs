use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::algorithm::native::eq::{line_string_eq, multi_line_string_eq};
use crate::geo_traits::{LineStringTrait, MultiLineStringTrait};
use crate::io::wkb::reader::coord::WKBCoord;
use crate::io::wkb::reader::geometry::Endianness;

const HEADER_BYTES: u64 = 5;

#[derive(Debug, Clone, Copy)]
pub struct WKBLineString<'a> {
    buf: &'a [u8],
    byte_order: Endianness,

    /// The number of points in this LineString WKB
    num_points: usize,

    /// This offset will be 0 for a single WKBLineString but it will be non zero for a
    /// WKBLineString contained within a WKBMultiLineString
    offset: u64,
}

impl<'a> WKBLineString<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(HEADER_BYTES + offset);
        let num_points = match byte_order {
            Endianness::BigEndian => reader.read_u32::<BigEndian>().unwrap().try_into().unwrap(),
            Endianness::LittleEndian => reader
                .read_u32::<LittleEndian>()
                .unwrap()
                .try_into()
                .unwrap(),
        };

        Self {
            buf,
            byte_order,
            num_points,
            offset,
        }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size(&self) -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numPoints
        // - 2 * 8 * self.num_points: two f64s for each coordinate
        1 + 4 + 4 + (2 * 8 * self.num_points as u64)
    }

    /// The offset into this buffer of any given coordinate
    pub fn coord_offset(&self, i: u64) -> u64 {
        self.offset + 1 + 4 + 4 + (2 * 8 * i)
    }

    /// Check if this WKBLineString has equal coordinates as some other LineString object
    pub fn equals_line_string(&self, other: &impl LineStringTrait<T = f64>) -> bool {
        line_string_eq(self, other)
    }

    /// Check if this WKBLineString has equal coordinates as some other MultiLineString object
    pub fn equals_multi_line_string(&self, other: &impl MultiLineStringTrait<T = f64>) -> bool {
        multi_line_string_eq(self, other)
    }
}

impl<'a> LineStringTrait for WKBLineString<'a> {
    type T = f64;
    type ItemType<'b> = WKBCoord<'a> where Self: 'b;

    fn num_coords(&self) -> usize {
        self.num_points
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        WKBCoord::new(
            self.buf,
            self.byte_order,
            self.coord_offset(i.try_into().unwrap()),
        )
    }
}

impl<'a> LineStringTrait for &'a WKBLineString<'a> {
    type T = f64;
    type ItemType<'b> = WKBCoord<'a> where Self: 'b;

    fn num_coords(&self) -> usize {
        self.num_points
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        WKBCoord::new(
            self.buf,
            self.byte_order,
            self.coord_offset(i.try_into().unwrap()),
        )
    }
}

impl<'a> MultiLineStringTrait for WKBLineString<'a> {
    type T = f64;
    type ItemType<'b> = WKBLineString<'a> where Self: 'b;

    fn num_lines(&self) -> usize {
        1
    }

    unsafe fn line_unchecked(&self, _i: usize) -> Self::ItemType<'_> {
        *self
    }
}

impl<'a> MultiLineStringTrait for &'a WKBLineString<'a> {
    type T = f64;
    type ItemType<'b> = WKBLineString<'a> where Self: 'b;

    fn num_lines(&self) -> usize {
        1
    }

    unsafe fn line_unchecked(&self, _i: usize) -> Self::ItemType<'_> {
        **self
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::linestring::ls0;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn line_string_round_trip() {
        let geom = ls0();
        let buf = geo::Geometry::LineString(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBLineString::new(&buf, Endianness::LittleEndian, 0);

        assert!(wkb_geom.equals_line_string(&geom));
    }

    #[test]
    fn test_size() {
        let geom = ls0();
        let buf = geo::Geometry::LineString(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBLineString::new(&buf, Endianness::LittleEndian, 0);

        assert_eq!(wkb_geom.size(), buf.len() as u64);
    }
}
