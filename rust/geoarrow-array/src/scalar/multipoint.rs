use arrow_buffer::OffsetBuffer;
use geo_traits::MultiPointTrait;
use geoarrow_schema::Dimension;

use crate::array::CoordBuffer;
use crate::eq::multi_point_eq;
use crate::scalar::Point;
use crate::util::OffsetBufferUtils;

/// An Arrow equivalent of a MultiPoint
///
/// This implements [MultiPointTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct MultiPoint<'a> {
    /// Buffer of coordinates
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> MultiPoint<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            geom_index,
            start_offset,
        }
    }
}

impl<'a> MultiPointTrait for MultiPoint<'a> {
    type T = f64;
    type PointType<'b>
        = Point<'a>
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

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        Point::new(self.coords, self.start_offset + i)
    }
}

impl<'a> MultiPointTrait for &'a MultiPoint<'a> {
    type T = f64;
    type PointType<'b>
        = Point<'a>
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

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        Point::new(self.coords, self.start_offset + i)
    }
}

impl<G: MultiPointTrait<T = f64>> PartialEq<G> for MultiPoint<'_> {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}

// #[cfg(test)]
// mod test {
//     use crate::array::MultiPointArray;
//     use crate::test::multipoint::{mp0, mp1};
//     use crate::trait_::ArrayAccessor;
//     use geoarrow_schema::Dimension;

//     /// Test Eq where the current index is true but another index is false
//     #[test]
//     fn test_eq_other_index_false() {
//         let arr1: MultiPointArray = (vec![mp0(), mp1()].as_slice(), Dimension::XY).into();
//         let arr2: MultiPointArray = (vec![mp0(), mp0()].as_slice(), Dimension::XY).into();

//         assert_eq!(arr1.value(0), arr2.value(0));
//         assert_ne!(arr1.value(1), arr2.value(1));
//     }
// }
