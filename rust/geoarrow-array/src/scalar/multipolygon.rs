use arrow_buffer::OffsetBuffer;
use geo_traits::MultiPolygonTrait;
use geoarrow_schema::Dimension;

use crate::array::CoordBuffer;
use crate::eq::multi_polygon_eq;
use crate::scalar::Polygon;
use crate::util::OffsetBufferUtils;

/// An Arrow equivalent of a MultiPolygon
///
/// This implements [MultiPolygonTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct MultiPolygon<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the polygon array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the ring array where each polygon starts
    pub(crate) polygon_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> MultiPolygon<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i32>,
        polygon_offsets: &'a OffsetBuffer<i32>,
        ring_offsets: &'a OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            geom_index,
            start_offset,
        }
    }
}

impl<'a> MultiPolygonTrait for MultiPolygon<'a> {
    type T = f64;
    type PolygonType<'b>
        = Polygon<'a>
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

    fn num_polygons(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        Polygon::new(
            self.coords,
            self.polygon_offsets,
            self.ring_offsets,
            self.start_offset + i,
        )
    }
}

impl<'a> MultiPolygonTrait for &'a MultiPolygon<'a> {
    type T = f64;
    type PolygonType<'b>
        = Polygon<'a>
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

    fn num_polygons(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        Polygon::new(
            self.coords,
            self.polygon_offsets,
            self.ring_offsets,
            self.start_offset + i,
        )
    }
}

impl<G: MultiPolygonTrait<T = f64>> PartialEq<G> for MultiPolygon<'_> {
    fn eq(&self, other: &G) -> bool {
        multi_polygon_eq(self, other)
    }
}

// #[cfg(test)]
// mod test {
//     use crate::array::MultiPolygonArray;
//     use crate::test::multipolygon::{mp0, mp1};
//     use crate::trait_::ArrayAccessor;
//     use geoarrow_schema::Dimension;

//     /// Test Eq where the current index is true but another index is false
//     #[test]
//     fn test_eq_other_index_false() {
//         let arr1: MultiPolygonArray = (vec![mp0(), mp1()].as_slice(), Dimension::XY).into();
//         let arr2: MultiPolygonArray = (vec![mp0(), mp0()].as_slice(), Dimension::XY).into();

//         assert_eq!(arr1.value(0), arr2.value(0));
//         assert_ne!(arr1.value(1), arr2.value(1));
//     }
// }
