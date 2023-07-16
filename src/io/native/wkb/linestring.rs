use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::{LineStringTrait, MultiLineStringTrait};
use crate::io::native::wkb::coord::WKBCoord;
use crate::io::native::wkb::geometry::Endianness;

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
        reader.set_position(HEADER_BYTES);
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
}

impl<'a> LineStringTrait<'a> for WKBLineString<'a> {
    type T = f64;
    type ItemType = WKBCoord<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_coords(&self) -> usize {
        self.num_points
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType> {
        if i > (self.num_points) {
            return None;
        }

        let coord = WKBCoord::new(
            self.buf,
            self.byte_order,
            self.coord_offset(i.try_into().unwrap()),
        );
        Some(coord)
    }

    fn coords(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a> LineStringTrait<'a> for &WKBLineString<'a> {
    type T = f64;
    type ItemType = WKBCoord<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_coords(&self) -> usize {
        self.num_points
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType> {
        if i > (self.num_points) {
            return None;
        }

        let offset = self.offset + 1 + 4 + 4 + (2 * 8 * i as u64);
        let coord = WKBCoord::new(self.buf, self.byte_order, offset);
        Some(coord)
    }

    fn coords(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a> MultiLineStringTrait<'a> for WKBLineString<'a> {
    type T = f64;
    type ItemType = WKBLineString<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_lines(&self) -> usize {
        1
    }

    fn line(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_lines() {
            return None;
        }

        Some(*self)
    }

    fn lines(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a> MultiLineStringTrait<'a> for &WKBLineString<'a> {
    type T = f64;
    type ItemType = WKBLineString<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_lines(&self) -> usize {
        1
    }

    fn line(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_lines() {
            return None;
        }

        Some(**self)
    }

    fn lines(&'a self) -> Self::Iter {
        todo!()
    }
}
