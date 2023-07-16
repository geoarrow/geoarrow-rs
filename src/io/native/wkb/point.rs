use crate::geo_traits::{CoordTrait, PointTrait};
use crate::io::native::wkb::coord::WKBCoord;
use crate::io::native::wkb::geometry::Endianness;

/// A 2D Point in WKB
///
/// See page 66 of https://portal.ogc.org/files/?artifact_id=25355.
#[derive(Clone, Copy)]
pub struct WKBPoint<'a> {
    /// The coordinate inside this WKBPoint
    coord: WKBCoord<'a>,
}

impl<'a> WKBPoint<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64) -> Self {
        // The space of the byte order + geometry type
        let offset = offset + 5;
        let coord = WKBCoord::new(buf, byte_order, offset);
        Self { coord }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size() -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numPoints
        // - 2 * 8: two f64s
        1 + 4 + (2 * 8)
    }
}

impl<'a> PointTrait for WKBPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        CoordTrait::x(&self.coord)
    }

    fn y(&self) -> Self::T {
        CoordTrait::y(&self.coord)
    }
}
