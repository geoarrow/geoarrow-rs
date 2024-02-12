use crate::algorithm::native::bounding_rect::bounding_rect_multipoint;
use crate::algorithm::native::eq::multi_point_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiPointArray};
use crate::geo_traits::MultiPointTrait;
use crate::io::geo::multi_point_to_geo;
use crate::scalar::Point;
use crate::trait_::GeometryArraySelfMethods;
use crate::trait_::GeometryScalarTrait;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a, O: OffsetSizeTrait> {
    /// Buffer of coordinates
    pub(crate) coords: Cow<'a, CoordBuffer>,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, O: OffsetSizeTrait> MultiPoint<'a, O> {
    pub fn new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
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

    pub fn new_borrowed(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self::new(
            Cow::Borrowed(coords),
            Cow::Borrowed(geom_offsets),
            geom_index,
        )
    }

    pub fn new_owned(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self::new(Cow::Owned(coords), Cow::Owned(geom_offsets), geom_index)
    }

    /// Extracts the owned data.
    ///
    /// Clones the data if it is not already owned.
    pub fn into_owned(self) -> Self {
        let arr = MultiPointArray::new(
            self.coords.into_owned(),
            self.geom_offsets.into_owned(),
            None,
            Default::default(),
        );
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        Self::new_owned(sliced_arr.coords, sliced_arr.geom_offsets, 0)
    }

    pub fn into_owned_inner(self) -> (CoordBuffer, OffsetBuffer<O>, usize) {
        let owned = self.into_owned();
        (
            owned.coords.into_owned(),
            owned.geom_offsets.into_owned(),
            owned.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for MultiPoint<'a, O> {
    type ScalarGeo = geo::MultiPoint;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, O: OffsetSizeTrait> MultiPointTrait for MultiPoint<'a, O> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point::new(self.coords.clone(), self.start_offset + i)
    }
}

impl<'a, O: OffsetSizeTrait> MultiPointTrait for &'a MultiPoint<'a, O> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point::new(self.coords.clone(), self.start_offset + i)
    }
}

impl<O: OffsetSizeTrait> From<MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_, O>) -> Self {
        multi_point_to_geo(value)
    }
}

impl<O: OffsetSizeTrait> From<MultiPoint<'_, O>> for geo::Geometry {
    fn from(value: MultiPoint<'_, O>) -> Self {
        geo::Geometry::MultiPoint(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for MultiPoint<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipoint(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> PartialEq<G> for MultiPoint<'_, O> {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPointArray;
    use crate::test::multipoint::{mp0, mp1};
    use crate::trait_::GeometryArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPointArray<i32> = vec![mp0(), mp1()].as_slice().into();
        let arr2: MultiPointArray<i32> = vec![mp0(), mp0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
