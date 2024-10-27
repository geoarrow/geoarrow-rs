use std::io::Cursor;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

use crate::datatypes::Dimension;
use crate::io::wkb::reader::coord::WKBCoord;
use crate::io::wkb::reader::geometry::Endianness;
use geo_traits::LineStringTrait;

/// A linear ring in a WKB buffer.
///
/// This has been preprocessed, so access to any internal coordinate is `O(1)`.
///
/// See page 65 of <https://portal.ogc.org/files/?artifact_id=25355>.
#[derive(Debug, Clone, Copy)]
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

    dim: Dimension,
}

impl<'a> WKBLinearRing<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64, dim: Dimension) -> Self {
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
            dim,
        }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size(&self) -> u64 {
        // - 4: numPoints
        // - 2 * 8 * self.num_points: two f64s for each coordinate
        4 + (self.dim.size() as u64 * 8 * self.num_points as u64)
    }

    /// The offset into this buffer of any given coordinate
    pub fn coord_offset(&self, i: u64) -> u64 {
        self.offset + 4 + (self.dim.size() as u64 * 8 * i)
    }
}

impl<'a> LineStringTrait for WKBLinearRing<'a> {
    type T = f64;
    type CoordType<'b> = WKBCoord<'a> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_coords(&self) -> usize {
        self.num_points
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        WKBCoord::new(
            self.buf,
            self.byte_order,
            self.coord_offset(i.try_into().unwrap()),
            self.dim,
        )
    }
}
