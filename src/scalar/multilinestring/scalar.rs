use crate::algorithm::native::bounding_rect::bounding_rect_multilinestring;
use crate::algorithm::native::eq::multi_line_string_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiLineStringArray};
use crate::geo_traits::MultiLineStringTrait;
use crate::io::geo::multi_line_string_to_geo;
use crate::scalar::LineString;
use crate::trait_::NativeScalar;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiLineString
#[derive(Debug, Clone)]
pub struct MultiLineString<'a, O: OffsetSizeTrait, const D: usize> {
    pub(crate) coords: &'a CoordBuffer<D>,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<O>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, O: OffsetSizeTrait, const D: usize> MultiLineString<'a, O, D> {
    pub fn new(
        coords: &'a CoordBuffer<D>,
        geom_offsets: &'a OffsetBuffer<O>,
        ring_offsets: &'a OffsetBuffer<O>,
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

    pub fn into_owned_inner(self) -> (CoordBuffer<D>, OffsetBuffer<O>, OffsetBuffer<O>, usize) {
        let arr = MultiLineStringArray::new(
            self.coords.clone(),
            self.geom_offsets.clone(),
            self.ring_offsets.clone(),
            None,
            Default::default(),
        );
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        (
            sliced_arr.coords,
            sliced_arr.geom_offsets,
            sliced_arr.ring_offsets,
            0,
        )
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> NativeScalar for MultiLineString<'a, O, D> {
    type ScalarGeo = geo::MultiLineString;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::MultiLineString(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> MultiLineStringTrait for MultiLineString<'a, O, D> {
    type T = f64;
    type ItemType<'b> = LineString<'a, O, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_lines(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> MultiLineStringTrait
    for &'a MultiLineString<'a, O, D>
{
    type T = f64;
    type ItemType<'b> = LineString<'a, O, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_lines(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<MultiLineString<'_, O, D>> for geo::MultiLineString {
    fn from(value: MultiLineString<'_, O, D>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<&MultiLineString<'_, O, D>> for geo::MultiLineString {
    fn from(value: &MultiLineString<'_, O, D>) -> Self {
        multi_line_string_to_geo(value)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<MultiLineString<'_, O, D>> for geo::Geometry {
    fn from(value: MultiLineString<'_, O, D>) -> Self {
        geo::Geometry::MultiLineString(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for MultiLineString<'_, O, 2> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multilinestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> PartialEq<G>
    for MultiLineString<'_, O, 2>
{
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiLineStringArray;
    use crate::test::multilinestring::{ml0, ml1};
    use crate::trait_::NativeArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiLineStringArray<i32, 2> = vec![ml0(), ml1()].as_slice().into();
        let arr2: MultiLineStringArray<i32, 2> = vec![ml0(), ml0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
