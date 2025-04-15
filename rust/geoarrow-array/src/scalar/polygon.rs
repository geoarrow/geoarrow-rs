use arrow_buffer::OffsetBuffer;
use geo_traits::PolygonTrait;
use geoarrow_schema::Dimension;

use crate::array::CoordBuffer;
use crate::eq::polygon_eq;
use crate::scalar::LineString;
use crate::util::OffsetBufferUtils;

/// An Arrow equivalent of a Polygon
///
/// This implements [PolygonTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct Polygon<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> Polygon<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i32>,
        ring_offsets: &'a OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
            start_offset,
        }
    }
}

impl<'a> PolygonTrait for Polygon<'a> {
    type T = f64;
    type RingType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            Dimension::XYM => geo_traits::Dimensions::Xym,
            Dimension::XYZM => geo_traits::Dimensions::Xyzm,
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(self.coords, self.ring_offsets, start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + 1 + i)
    }
}

impl<'a> PolygonTrait for &'a Polygon<'a> {
    type T = f64;
    type RingType<'b>
        = LineString<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            Dimension::XYM => geo_traits::Dimensions::Xym,
            Dimension::XYZM => geo_traits::Dimensions::Xyzm,
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(self.coords, self.ring_offsets, start))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + 1 + i)
    }
}

impl<G: PolygonTrait<T = f64>> PartialEq<G> for Polygon<'_> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}

// #[cfg(test)]
// mod test {
//     use crate::array::PolygonArray;
//     use crate::test::polygon::{p0, p1};
//     use crate::trait_::ArrayAccessor;
//     use geoarrow_schema::Dimension;

//     /// Test Eq where the current index is true but another index is false
//     #[test]
//     fn test_eq_other_index_false() {
//         let arr1: PolygonArray = (vec![p0(), p1()].as_slice(), Dimension::XY).into();
//         let arr2: PolygonArray = (vec![p0(), p0()].as_slice(), Dimension::XY).into();

//         assert_eq!(arr1.value(0), arr2.value(0));
//         assert_ne!(arr1.value(1), arr2.value(1));
//     }
// }
