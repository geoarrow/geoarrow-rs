use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::MultiLineStringTrait;
use crate::io::native::wkb::geometry::Endianness;
use crate::io::native::wkb::linestring::WKBLineString;

const HEADER_BYTES: u64 = 5;
const F64_WIDTH: u64 = 8;

pub struct WKBMultiLineString<'a> {
    buf: &'a [u8],
    byte_order: Endianness,

    /// The number of line strings in this MultiLineString
    num_line_strings: usize,
}

impl<'a> WKBMultiLineString<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness) -> Self {
        let mut reader = Cursor::new(buf);
        reader.set_position(HEADER_BYTES);
        let num_line_strings = match byte_order {
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
            num_line_strings,
        }
    }
}

impl<'a> MultiLineStringTrait<'a> for WKBMultiLineString<'a> {
    type ItemType = WKBLineString<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_lines(&'a self) -> usize {
        self.num_line_strings
    }

    fn line(&'a self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_lines() {
            return None;
        }

        let offset = 1 + 4 + 4 + (2 * F64_WIDTH * i as u64);
        Some(WKBLineString::new(self.buf, self.byte_order, offset))
    }

    fn lines(&'a self) -> Self::Iter {
        todo!()
    }
}
