use std::io::Cursor;
use std::iter::Cloned;
use std::slice::Iter;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::MultiLineStringTrait;
use crate::io::native::wkb::geometry::Endianness;
use crate::io::native::wkb::linestring::WKBLineString;

const HEADER_BYTES: u64 = 5;

#[derive(Debug, Clone)]
pub struct WKBMultiLineString<'a> {
    /// A WKBLineString object for each of the internal line strings
    wkb_line_strings: Vec<WKBLineString<'a>>,
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

        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numLineStrings
        let mut line_string_offset = 1 + 4 + 4;
        let mut wkb_line_strings = Vec::with_capacity(num_line_strings);
        for _ in 0..num_line_strings {
            let ls = WKBLineString::new(buf, byte_order, line_string_offset);
            wkb_line_strings.push(ls);
            line_string_offset += ls.size();
        }

        Self { wkb_line_strings }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size(&self) -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numPoints
        // - WKBPoint::size() * self.num_points: the size of each WKBPoint for each point
        self.wkb_line_strings
            .iter()
            .fold(1 + 4 + 4, |acc, ls| acc + ls.size())
    }
}

impl<'a> MultiLineStringTrait<'a> for WKBMultiLineString<'a> {
    type T = f64;
    type ItemType = WKBLineString<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_lines(&self) -> usize {
        self.wkb_line_strings.len()
    }

    fn line(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_lines() {
            return None;
        }

        Some(self.wkb_line_strings[i])
    }

    fn lines(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a> MultiLineStringTrait<'a> for &WKBMultiLineString<'a> {
    type T = f64;
    type ItemType = WKBLineString<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_lines(&self) -> usize {
        self.wkb_line_strings.len()
    }

    fn line(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_lines() {
            return None;
        }

        Some(self.wkb_line_strings[i])
    }

    fn lines(&'a self) -> Self::Iter {
        todo!()
    }
}
