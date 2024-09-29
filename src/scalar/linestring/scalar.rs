use crate::algorithm::native::bounding_rect::bounding_rect_linestring;
use crate::algorithm::native::eq::line_string_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, LineStringArray};
use crate::geo_traits::LineStringTrait;
use crate::io::geo::line_string_to_geo;
use crate::scalar::Point;
use crate::trait_::NativeScalar;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a LineString
#[derive(Debug, Clone)]
pub struct LineString<'a, const D: usize> {
    pub(crate) coords: &'a CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, const D: usize> LineString<'a, D> {
    pub fn new(coords: &'a CoordBuffer<D>, geom_offsets: &'a OffsetBuffer<i32>, geom_index: usize) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self { coords, geom_offsets, geom_index, start_offset }
    }

    pub fn into_owned_inner(self) -> (CoordBuffer<D>, OffsetBuffer<i32>, usize) {
        let arr = LineStringArray::new(self.coords.clone(), self.geom_offsets.clone(), None, Default::default());
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        let (coords, geom_offsets, _validity) = sliced_arr.into_inner();
        (coords, geom_offsets, 0)
    }
}

impl<'a, const D: usize> NativeScalar for LineString<'a, D> {
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

impl<'a, const D: usize> LineStringTrait for LineString<'a, D> {
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
        Point::new(self.coords, self.start_offset + i)
    }
}

impl<'a, const D: usize> LineStringTrait for &'a LineString<'a, D> {
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
        Point::new(self.coords, self.start_offset + i)
    }
}

impl<const D: usize> From<LineString<'_, D>> for geo::LineString {
    fn from(value: LineString<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&LineString<'_, D>> for geo::LineString {
    fn from(value: &LineString<'_, D>) -> Self {
        line_string_to_geo(value)
    }
}

impl<const D: usize> From<LineString<'_, D>> for geo::Geometry {
    fn from(value: LineString<'_, D>) -> Self {
        geo::Geometry::LineString(value.into())
    }
}

impl RTreeObject for LineString<'_, 2> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_linestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: LineStringTrait<T = f64>> PartialEq<G> for LineString<'_, 2> {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::LineStringArray;
    use crate::test::linestring::{ls0, ls1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: LineStringArray<i32, 2> = vec![ls0(), ls1()].as_slice().into();
        let arr2: LineStringArray<i32, 2> = vec![ls0(), ls0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
