use crate::algorithm::native::bounding_rect::bounding_rect_multipoint;
use crate::array::multipoint::MultiPointIterator;
use crate::array::CoordBuffer;
use crate::geo_traits::MultiPointTrait;
use crate::scalar::Point;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a, O: Offset> {
    /// Buffer of coordinates
    pub coords: Cow<'a, CoordBuffer>,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: Cow<'a, OffsetsBuffer<O>>,

    pub geom_index: usize,
}

impl<'a, O: Offset> MultiPoint<'a, O> {
    pub fn new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetsBuffer<O>>,
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
        geom_offsets: &'a OffsetsBuffer<O>,
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
        geom_offsets: OffsetsBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords: Cow::Owned(coords),
            geom_offsets: Cow::Owned(geom_offsets),
            geom_index,
        }
    }
}

impl<'a, O: Offset> GeometryScalarTrait<'a> for MultiPoint<'a, O> {
    type ScalarGeo = geo::MultiPoint;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: Offset> MultiPointTrait<'a> for MultiPoint<'a, O> {
    type T = f64;
    type ItemType = Point<'a>;
    type Iter = MultiPointIterator<'a, O>;

    fn points(&'a self) -> Self::Iter {
        MultiPointIterator::new(self)
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn point(&self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(Point::new(self.coords.clone(), start + i))
    }
}

impl<O: Offset> From<MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: Offset> From<&MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_, O>) -> Self {
        let (start_idx, end_idx) = value.geom_offsets.start_end(value.geom_index);
        let mut coords: Vec<geo::Point> = Vec::with_capacity(end_idx - start_idx);

        for i in start_idx..end_idx {
            coords.push(value.coords.value(i).into());
        }

        geo::MultiPoint::new(coords)
    }
}

impl<O: Offset> From<MultiPoint<'_, O>> for geo::Geometry {
    fn from(value: MultiPoint<'_, O>) -> Self {
        geo::Geometry::MultiPoint(value.into())
    }
}

impl<O: Offset> RTreeObject for MultiPoint<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipoint(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: Offset> PartialEq for MultiPoint<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        let mut left_coords = self.coords.clone();
        let (left_start, left_end) = self.geom_offsets.start_end(self.geom_index);
        left_coords
            .to_mut()
            .slice(left_start, left_end - left_start);

        let mut right_coords = other.coords.clone();
        let (right_start, right_end) = other.geom_offsets.start_end(other.geom_index);
        right_coords
            .to_mut()
            .slice(right_start, right_end - right_start);

        left_coords == right_coords
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPointArray;
    use crate::test::multipoint::{mp0, mp1};
    use crate::GeometryArrayTrait;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPointArray<i32> = vec![mp0(), mp1()].into();
        let arr2: MultiPointArray<i32> = vec![mp0(), mp0()].into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
