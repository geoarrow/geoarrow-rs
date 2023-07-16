use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::MultiPointTrait;
use crate::io::native::wkb::geometry::Endianness;
use crate::io::native::wkb::point::WKBPoint;

pub struct WKBMultiPoint<'a> {
    buf: &'a [u8],
    byte_order: Endianness,

    /// The number of points in this multi point
    num_points: usize,
}

impl<'a> WKBMultiPoint<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness) -> Self {
        // TODO: assert WKB type?
        let mut reader = Cursor::new(buf);
        // Set reader to after 1-byte byteOrder and 4-byte wkbType
        reader.set_position(1 + 4);
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
        }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size(&self) -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numPoints
        // - WKBPoint::size() * self.num_points: the size of each WKBPoint for each point
        1 + 4 + 4 + (WKBPoint::size() * self.num_points as u64)
    }

    /// The offset into this buffer of any given WKBPoint
    pub fn point_offset(&self, i: u64) -> u64 {
        1 + 4 + 4 + (WKBPoint::size() * i)
    }
}

impl<'a> MultiPointTrait<'a> for WKBMultiPoint<'a> {
    type T = f64;
    type ItemType = WKBPoint<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_points(&self) -> usize {
        self.num_points
    }

    fn point(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_points() {
            return None;
        }

        Some(WKBPoint::new(
            self.buf,
            self.byte_order,
            self.point_offset(i.try_into().unwrap()),
        ))
    }

    fn points(&'a self) -> Self::Iter {
        todo!()
    }
}
