use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::MultiPolygonTrait;
use crate::io::native::wkb::geometry::Endianness;
use crate::io::native::wkb::polygon::WKBPolygon;

const HEADER_BYTES: u64 = 5;
const F64_WIDTH: u64 = 8;

pub struct WKBMultiPolygon<'a> {
    buf: &'a [u8],
    byte_order: Endianness,

    /// The number of polygons in this MultiPolygon
    num_polygons: usize,
}

impl<'a> WKBMultiPolygon<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(HEADER_BYTES);
        let num_polygons = match byte_order {
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
            num_polygons,
        }
    }
}

impl<'a> MultiPolygonTrait<'a> for WKBMultiPolygon<'a> {
    type T = f64;
    type ItemType = WKBPolygon<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_polygons(&self) -> usize {
        self.num_polygons
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_polygons() {
            return None;
        }

        // TODO: this offset is wrong!
        let offset = 1 + 4 + 4 + (2 * F64_WIDTH * i as u64);
        Some(WKBPolygon::new(self.buf, self.byte_order, offset))
    }

    fn polygons(&'a self) -> Self::Iter {
        todo!()
    }
}
