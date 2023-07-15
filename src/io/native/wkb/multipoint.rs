use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::MultiPointTrait;
use crate::io::native::wkb::geometry::Endianness;
use crate::io::native::wkb::point::WKBPoint;

const HEADER_BYTES: u64 = 5;
const F64_WIDTH: u64 = 8;

pub struct WKBMultiPoint<'a> {
    buf: &'a [u8],
    byte_order: Endianness,

    /// The number of points in this multi point
    num_points: usize,
}

impl<'a> WKBMultiPoint<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness) -> Self {
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
        }
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

        let offset = 1 + 4 + 4 + (2 * F64_WIDTH * i as u64);
        Some(WKBPoint::new(self.buf, self.byte_order, offset))
    }

    fn points(&'a self) -> Self::Iter {
        todo!()
    }
}
