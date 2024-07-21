use crate::algorithm::native::bounding_rect::bounding_rect_linestring;
use crate::algorithm::native::eq::line_string_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, LineStringArray};
use crate::geo_traits::LineStringTrait;
use crate::io::geo::line_string_to_geo;
use crate::scalar::Point;
use crate::trait_::{GeometryArraySelfMethods, GeometryScalarTrait};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a LineString
#[derive(Debug, Clone)]
pub struct LineString<'a, O: OffsetSizeTrait, const D: usize> {
    pub(crate) coords: Cow<'a, CoordBuffer<D>>,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, O: OffsetSizeTrait, const D: usize> LineString<'a, O, D> {
    pub fn new(
        coords: Cow<'a, CoordBuffer<D>>,
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
        coords: &'a CoordBuffer<D>,
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
        coords: CoordBuffer<D>,
        geom_offsets: OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self::new(Cow::Owned(coords), Cow::Owned(geom_offsets), geom_index)
    }

    /// Extracts the owned data.
    ///
    /// Clones the data if it is not already owned.
    pub fn into_owned(self) -> Self {
        let arr = LineStringArray::new(
            self.coords.into_owned(),
            self.geom_offsets.into_owned(),
            None,
            Default::default(),
        );
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        Self::new_owned(sliced_arr.coords, sliced_arr.geom_offsets, 0)
    }

    pub fn into_owned_inner(self) -> (CoordBuffer<D>, OffsetBuffer<O>, usize) {
        let owned = self.into_owned();
        (
            owned.coords.into_owned(),
            owned.geom_offsets.into_owned(),
            owned.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> GeometryScalarTrait for LineString<'a, O, D> {
    type ScalarGeo = geo::LineString;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::LineString(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> LineStringTrait for LineString<'a, O, D> {
    type T = f64;
    type ItemType<'b> = Point<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point::new(self.coords.clone(), self.start_offset + i)
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> LineStringTrait for &'a LineString<'a, O, D> {
    type T = f64;
    type ItemType<'b> = Point<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point::new(self.coords.clone(), self.start_offset + i)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<LineString<'_, O, D>> for geo::LineString {
    fn from(value: LineString<'_, O, D>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<&LineString<'_, O, D>> for geo::LineString {
    fn from(value: &LineString<'_, O, D>) -> Self {
        line_string_to_geo(value)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<LineString<'_, O, D>> for geo::Geometry {
    fn from(value: LineString<'_, O, D>) -> Self {
        geo::Geometry::LineString(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for LineString<'_, O, 2> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_linestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> PartialEq<G> for LineString<'_, O, 2> {
    fn eq(&self, other: &G) -> bool {
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
        let arr1: LineStringArray<i32, 2> = vec![ls0(), ls1()].as_slice().into();
        let arr2: LineStringArray<i32, 2> = vec![ls0(), ls0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
