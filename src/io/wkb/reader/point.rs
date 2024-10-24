use crate::algorithm::native::eq::point_eq;
use crate::datatypes::Dimension;
use crate::geo_traits::{CoordTrait, MultiPointTrait, PointTrait};
use crate::io::wkb::reader::coord::WKBCoord;
use crate::io::wkb::reader::geometry::Endianness;

/// A WKB Point.
///
/// This has been preprocessed, so access to any internal coordinate is `O(1)`.
///
/// See page 66 of <https://portal.ogc.org/files/?artifact_id=25355>.
#[derive(Debug, Clone, Copy)]
pub struct WKBPoint<'a> {
    /// The coordinate inside this WKBPoint
    coord: WKBCoord<'a>,
    dim: Dimension,
    is_empty: bool,
}

impl<'a> WKBPoint<'a> {
    pub fn new(buf: &'a [u8], byte_order: Endianness, offset: u64, dim: Dimension) -> Self {
        // The space of the byte order + geometry type
        let offset = offset + 5;
        let coord = WKBCoord::new(buf, byte_order, offset, dim);
        let is_empty =
            (0..coord.dim().size()).all(|coord_dim| coord.nth_unchecked(coord_dim).is_nan());
        Self {
            coord,
            dim,
            is_empty,
        }
    }

    /// The number of bytes in this object, including any header
    ///
    /// Note that this is not the same as the length of the underlying buffer
    pub fn size(&self) -> u64 {
        // - 1: byteOrder
        // - 4: wkbType
        // - 4: numPoints
        // - dim size * 8: two f64s
        1 + 4 + (self.dim.size() as u64 * 8)
    }

    /// Check if this WKBPoint has equal coordinates as some other Point object
    pub fn equals_point(&self, other: &impl PointTrait<T = f64>) -> bool {
        point_eq(self, other)
    }

    pub fn dimension(&self) -> Dimension {
        self.dim
    }
}

impl<'a> PointTrait for WKBPoint<'a> {
    type T = f64;
    type CoordType<'b> = WKBCoord<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        if self.is_empty {
            None
        } else {
            Some(self.coord)
        }
    }
}

impl<'a> PointTrait for &WKBPoint<'a> {
    type T = f64;
    type CoordType<'b> = WKBCoord<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        if self.is_empty {
            None
        } else {
            Some(self.coord)
        }
    }
}

impl<'a> MultiPointTrait for WKBPoint<'a> {
    type T = f64;
    type PointType<'b> = WKBPoint<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_points(&self) -> usize {
        1
    }

    unsafe fn point_unchecked(&self, _i: usize) -> Self::PointType<'_> {
        *self
    }
}

impl<'a> MultiPointTrait for &'a WKBPoint<'a> {
    type T = f64;
    type PointType<'b> = WKBPoint<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        self.dim.into()
    }

    fn num_points(&self) -> usize {
        1
    }

    unsafe fn point_unchecked(&self, _i: usize) -> Self::PointType<'_> {
        **self
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::p0;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn point_round_trip() {
        let point = p0();
        let buf = geo::Geometry::Point(point)
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_point = WKBPoint::new(&buf, Endianness::LittleEndian, 0, Dimension::XY);

        assert!(wkb_point.equals_point(&point));
    }
}
