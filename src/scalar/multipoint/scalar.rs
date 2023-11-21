use crate::algorithm::native::bounding_rect::bounding_rect_multipoint;
use crate::algorithm::native::eq::multi_point_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiPointArray};
use crate::geo_traits::MultiPointTrait;
use crate::scalar::multipoint::MultiPointIterator;
use crate::scalar::Point;
use crate::trait_::GeoArrayAccessor;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a, O: OffsetSizeTrait> {
    /// Buffer of coordinates
    pub coords: Cow<'a, CoordBuffer>,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: Cow<'a, OffsetBuffer<O>>,

    pub geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> MultiPoint<'a, O> {
    pub fn new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }

    pub fn new_borrowed(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords: Cow::Borrowed(coords),
            geom_offsets: Cow::Borrowed(geom_offsets),
            geom_index,
        }
    }

    pub fn new_owned(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords: Cow::Owned(coords),
            geom_offsets: Cow::Owned(geom_offsets),
            geom_index,
        }
    }

    /// Extracts the owned data.
    ///
    /// Clones the data if it is not already owned.
    pub fn into_owned(self) -> Self {
        let arr = MultiPointArray::new(
            self.coords.into_owned(),
            self.geom_offsets.into_owned(),
            None,
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

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait<'a> for MultiPoint<'a, O> {
    type ScalarGeo = geo::MultiPoint;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: OffsetSizeTrait> MultiPointTrait for MultiPoint<'a, O> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;
    type Iter<'b> = MultiPointIterator<'a, O> where Self: 'b;

    fn points(&self) -> Self::Iter<'_> {
        todo!()
        // MultiPointIterator::new(self)
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(Point::new(self.coords.clone(), start + i))
    }
}

impl<'a, O: OffsetSizeTrait> MultiPointTrait for &'a MultiPoint<'a, O> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;
    type Iter<'b> = MultiPointIterator<'a, O> where Self: 'b;

    fn points(&self) -> Self::Iter<'_> {
        MultiPointIterator::new(self)
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(Point::new(self.coords.clone(), start + i))
    }
}

impl<O: OffsetSizeTrait> From<MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_, O>) -> Self {
        let (start_idx, end_idx) = value.geom_offsets.start_end(value.geom_index);
        let mut coords: Vec<geo::Point> = Vec::with_capacity(end_idx - start_idx);

        for i in start_idx..end_idx {
            coords.push(value.coords.value(i).into());
        }

        geo::MultiPoint::new(coords)
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

impl<O: OffsetSizeTrait> PartialEq for MultiPoint<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        multi_point_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPointArray;
    use crate::test::multipoint::{mp0, mp1};
    use crate::trait_::GeoArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPointArray<i32> = vec![mp0(), mp1()].into();
        let arr2: MultiPointArray<i32> = vec![mp0(), mp0()].into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
