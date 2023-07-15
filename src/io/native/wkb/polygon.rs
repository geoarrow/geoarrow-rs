use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::PolygonTrait;
use crate::io::native::wkb::geometry::Endianness;
use crate::io::native::wkb::linearring::WKBLinearRing;

const HEADER_BYTES: u64 = 5;
const F64_WIDTH: u64 = 8;

#[derive(Clone, Copy)]
pub struct WKBPolygon<'a> {
    buf: &'a [u8],
    byte_order: Endianness,

    /// The number of rings in this polygon
    num_rings: usize,

    /// This offset will be 0 for a single WKBPolygon but it will be non zero for a
    /// WKBPolygon contained within a WKBMultiPolygon
    offset: u64,
}

impl<'a> WKBPolygon<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(HEADER_BYTES);
        let num_rings = match byte_order {
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
            num_rings,
            offset,
        }
    }
}

impl<'a> PolygonTrait<'a> for WKBPolygon<'a> {
    type T = f64;
    type ItemType = WKBLinearRing<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_interiors(&self) -> usize {
        self.num_rings - 1
    }

    fn exterior(&self) -> Self::ItemType {
        // Here the exterior is always the first linear ring so it starts right after the header
        let offset = self.offset + 1 + 4 + 4;
        WKBLinearRing::new(self.buf, self.byte_order, offset)
    }

    fn interior(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_interiors() {
            return None;
        }

        let offset = self.offset + 1 + 4 + 4 + (2 * F64_WIDTH * (i as u64 + 1));
        Some(WKBLinearRing::new(self.buf, self.byte_order, offset))
    }

    fn interiors(&'a self) -> Self::Iter {
        todo!()
    }
}
