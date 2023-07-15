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
}

impl<'a> PointTrait for WKBPoint<'a> {
    fn x(&self) -> f64 {
        CoordTrait::x(&self.coord)
    }

    fn y(&self) -> f64 {
        CoordTrait::y(&self.coord)
    }
}
