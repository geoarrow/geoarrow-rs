use crate::geo_traits::{CoordTrait, MultiPointTrait, PointTrait};
use crate::io::native::wkb::coord::WKBCoord;
use crate::io::native::wkb::geometry::Endianness;
use std::iter::Cloned;
use std::slice::Iter;

/// A 2D Point in WKB
///
/// See page 66 of <https://portal.ogc.org/files/?artifact_id=25355>.
#[derive(Debug, Clone, Copy)]
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

impl<'a> PointTrait for &WKBPoint<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        CoordTrait::x(&self.coord)
    }

    fn y(&self) -> Self::T {
        CoordTrait::y(&self.coord)
    }
}

impl<'a> MultiPointTrait<'a> for WKBPoint<'a> {
    type T = f64;
    type ItemType = WKBPoint<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_points(&self) -> usize {
        1
    }

    fn point(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_points() {
            return None;
        }

        Some(*self)
    }

    fn points(&'a self) -> Self::Iter {
        todo!()
    }
}

impl<'a> MultiPointTrait<'a> for &WKBPoint<'a> {
    type T = f64;
    type ItemType = WKBPoint<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_points(&self) -> usize {
        1
    }

    fn point(&self, i: usize) -> Option<Self::ItemType> {
        if i > self.num_points() {
            return None;
        }

        Some(**self)
    }

    fn points(&'a self) -> Self::Iter {
        todo!()
    }
}
