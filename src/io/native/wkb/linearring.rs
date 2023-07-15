use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::LineStringTrait;
use crate::io::native::wkb::coord::WKBCoord;
use crate::io::native::wkb::geometry::Endianness;

const F64_WIDTH: u64 = 8;

/// A linear ring in a WKB buffer.
///
/// See page 65 of https://portal.ogc.org/files/?artifact_id=25355.
#[derive(Clone, Copy)]
pub struct WKBLinearRing<'a> {
    /// The underlying WKB buffer
    buf: &'a [u8],

    /// The byte order of this WKB buffer
    byte_order: Endianness,

    /// The offset into the buffer where this linear ring is located
    ///
    /// Note that this does not have to be immediately after the WKB header! For a `WKBPoint`, the
    /// `Point` is immediately after the header, but the `Point` also appears in other geometry
    /// types. I.e. the `WKBLineString` has a header, then the number of points, then a sequence of
    /// `Point` objects.
    offset: u64,

    /// The number of points in this linear ring
    num_points: usize,
}

impl<'a> WKBLinearRing<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(offset);
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
            offset,
            num_points,
        }
    }
}

impl<'a> LineStringTrait<'a> for WKBLinearRing<'a> {
    type ItemType = WKBCoord<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_points(&'a self) -> usize {
        self.num_points
    }

    fn point(&'a self, i: usize) -> Option<Self::ItemType> {
        if i > (self.num_points) {
            return None;
        }

        let offset = self.offset + 4 + (2 * F64_WIDTH * i as u64);
        let coord = WKBCoord::new(self.buf, self.byte_order, offset);
        Some(coord)
    }

    fn points(&'a self) -> Self::Iter {
        todo!()
    }
}
