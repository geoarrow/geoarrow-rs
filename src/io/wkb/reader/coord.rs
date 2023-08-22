use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::geo_traits::{CoordTrait, PointTrait};
use crate::io::wkb::reader::geometry::Endianness;

const F64_WIDTH: u64 = 8;

/// A coordinate in a WKB buffer.
///
/// Note that according to the WKB specification this is called `Point`, which is **not** the same
/// as a `WKBPoint`. In particular, a `WKBPoint` has framing that includes the byte order and
/// geometry type of the WKB buffer. In contrast, this `Point` is the building block of two f64
/// numbers that can occur within any geometry type.
///
/// See page 65 of <https://portal.ogc.org/files/?artifact_id=25355>.
#[derive(Debug, Clone, Copy)]
pub struct WKBCoord<'a> {
    /// The underlying WKB buffer
    buf: &'a [u8],

    /// The byte order of this WKB buffer
    byte_order: Endianness,

    /// The offset into the buffer where this coordinate is located
    ///
    /// Note that this does not have to be immediately after the WKB header! For a `WKBPoint`, the
    /// `Point` is immediately after the header, but the `Point` also appears in other geometry
    /// types. I.e. the `WKBLineString` has a header, then the number of points, then a sequence of
    /// `Point` objects.
    offset: u64,
}

impl<'a> WKBCoord<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64) -> Self {
        Self {
            buf,
            byte_order,
            offset,
        }
    }

    fn get_x(&self) -> f64 {
        let mut reader = Cursor::new(self.buf);
        reader.set_position(self.offset);
        match self.byte_order {
            Endianness::BigEndian => reader.read_f64::<BigEndian>().unwrap(),
            Endianness::LittleEndian => reader.read_f64::<LittleEndian>().unwrap(),
        }
    }

    fn get_y(&self) -> f64 {
        let mut reader = Cursor::new(self.buf);
        reader.set_position(self.offset + F64_WIDTH);
        match self.byte_order {
            Endianness::BigEndian => reader.read_f64::<BigEndian>().unwrap(),
            Endianness::LittleEndian => reader.read_f64::<LittleEndian>().unwrap(),
        }
    }

    /// The number of bytes in this object
    ///
    /// Note that this is not the same as the length of the underlying buffer
    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        // A 2D WKBCoord is just two f64s
        16
    }
}

impl<'a> CoordTrait for WKBCoord<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.get_x()
    }

    fn y(&self) -> Self::T {
        self.get_y()
    }
}

impl<'a> PointTrait for WKBCoord<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.get_x()
    }

    fn y(&self) -> Self::T {
        self.get_y()
    }
}
