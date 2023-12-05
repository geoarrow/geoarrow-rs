use crate::algorithm::native::bounding_rect::bounding_rect_linestring;
use crate::algorithm::native::eq::line_string_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, LineStringArray};
use crate::geo_traits::LineStringTrait;
use crate::scalar::Point;
use crate::trait_::{GeometryArraySelfMethods, GeometryScalarTrait};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

use crate::scalar::linestring::LineStringIterator;

/// An Arrow equivalent of a LineString
#[derive(Debug, Clone)]
pub struct LineString<'a, O: OffsetSizeTrait> {
    pub coords: Cow<'a, CoordBuffer>,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: Cow<'a, OffsetBuffer<O>>,

    pub geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> LineString<'a, O> {
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
        let arr = LineStringArray::new(
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

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for LineString<'a, O> {
    type ScalarGeo = geo::LineString;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: OffsetSizeTrait> LineStringTrait for LineString<'a, O> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;
    type Iter<'b> = LineStringIterator<'a, O> where Self: 'b;

    fn coords(&self) -> Self::Iter<'_> {
        todo!()
        // LineStringIterator::new(self)
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(Point::new(self.coords.clone(), start + i))
    }
}

impl<'a, O: OffsetSizeTrait> LineStringTrait for &'a LineString<'a, O> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;
    type Iter<'b> = LineStringIterator<'a, O> where Self: 'b;

    fn coords(&self) -> Self::Iter<'_> {
        LineStringIterator::new(self)
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn coord(&self, i: usize) -> Option<Self::ItemType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(Point::new(self.coords.clone(), start + i))
    }
}

impl<O: OffsetSizeTrait> From<LineString<'_, O>> for geo::LineString {
    fn from(value: LineString<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&LineString<'_, O>> for geo::LineString {
    fn from(value: &LineString<'_, O>) -> Self {
        let num_coords = value.num_coords();
        let mut coords: Vec<geo::Coord> = Vec::with_capacity(num_coords);

        for i in 0..num_coords {
            coords.push(value.coord(i).unwrap().into());
        }

        geo::LineString::new(coords)
    }
}

impl<O: OffsetSizeTrait> From<LineString<'_, O>> for geo::Geometry {
    fn from(value: LineString<'_, O>) -> Self {
        geo::Geometry::LineString(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for LineString<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_linestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait> PartialEq for LineString<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        line_string_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::LineStringArray;
    use crate::test::linestring::{ls0, ls1};
    use crate::trait_::GeometryArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: LineStringArray<i32> = vec![ls0(), ls1()].as_slice().into();
        let arr2: LineStringArray<i32> = vec![ls0(), ls0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
