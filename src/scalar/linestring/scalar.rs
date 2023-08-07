use crate::algorithm::native::bounding_rect::bounding_rect_linestring;
use crate::array::CoordBuffer;
use crate::geo_traits::LineStringTrait;
use crate::scalar::Point;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

use crate::array::linestring::LineStringIterator;

/// An Arrow equivalent of a LineString
#[derive(Debug, Clone)]
pub struct LineString<'a, O: Offset> {
    pub coords: &'a CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<O>,

    pub geom_index: usize,
}

impl<'a, O: Offset> GeometryScalarTrait<'a> for LineString<'a, O> {
    type ScalarGeo = geo::LineString;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: Offset> LineStringTrait<'a> for LineString<'a, O> {
    type T = f64;
    type ItemType = Point<'a>;
    type Iter = LineStringIterator<'a, O>;

    fn coords(&'a self) -> Self::Iter {
        LineStringIterator::new(self)
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        let point = Point {
            coords: self.coords,
            geom_index: start + i,
        };
        Some(point)
    }
}

impl<'a, O: Offset> LineStringTrait<'a> for &LineString<'a, O> {
    type T = f64;
    type ItemType = Point<'a>;
    type Iter = LineStringIterator<'a, O>;

    fn coords(&'a self) -> Self::Iter {
        LineStringIterator::new(self)
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        let point = Point {
            coords: self.coords,
            geom_index: start + i,
        };
        Some(point)
    }
}

impl<O: Offset> From<LineString<'_, O>> for geo::LineString {
    fn from(value: LineString<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: Offset> From<&LineString<'_, O>> for geo::LineString {
    fn from(value: &LineString<'_, O>) -> Self {
        let num_coords = value.num_coords();
        let mut coords: Vec<geo::Coord> = Vec::with_capacity(num_coords);

        for i in 0..num_coords {
            coords.push(value.coord(i).unwrap().into());
        }

        geo::LineString::new(coords)
    }
}

impl<O: Offset> From<LineString<'_, O>> for geo::Geometry {
    fn from(value: LineString<'_, O>) -> Self {
        geo::Geometry::LineString(value.into())
    }
}

impl<O: Offset> RTreeObject for LineString<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_linestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: Offset> PartialEq for LineString<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        let mut left_coords = self.coords.clone();
        let (left_start, left_end) = self.geom_offsets.start_end(self.geom_index);
        left_coords.slice(left_start, left_end - left_start);

        let mut right_coords = other.coords.clone();
        let (right_start, right_end) = other.geom_offsets.start_end(other.geom_index);
        right_coords.slice(right_start, right_end - right_start);

        left_coords == right_coords
    }
}

#[cfg(test)]
mod test {
    use crate::array::LineStringArray;
    use crate::test::linestring::{ls0, ls1};
    use crate::GeometryArrayTrait;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: LineStringArray<i32> = vec![ls0(), ls1()].into();
        let arr2: LineStringArray<i32> = vec![ls0(), ls0()].into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
