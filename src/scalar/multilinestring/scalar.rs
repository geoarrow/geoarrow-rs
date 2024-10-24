use crate::algorithm::native::bounding_rect::bounding_rect_multilinestring;
use crate::algorithm::native::eq::multi_line_string_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiLineStringArray};
use crate::geo_traits::MultiLineStringTrait;
use crate::io::geo::multi_line_string_to_geo;
use crate::scalar::LineString;
use crate::trait_::NativeScalar;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiLineString
#[derive(Debug, Clone)]
pub struct MultiLineString<'a, const D: usize> {
    pub(crate) coords: &'a CoordBuffer<D>,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, const D: usize> MultiLineString<'a, D> {
    pub fn new(
        coords: &'a CoordBuffer<D>,
        geom_offsets: &'a OffsetBuffer<i32>,
        ring_offsets: &'a OffsetBuffer<i32>,
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

    pub fn into_owned_inner(self) -> (CoordBuffer<D>, OffsetBuffer<i32>, OffsetBuffer<i32>, usize) {
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

impl<'a, const D: usize> NativeScalar for MultiLineString<'a, D> {
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

impl<'a, const D: usize> MultiLineStringTrait for MultiLineString<'a, D> {
    type T = f64;
    type LineStringType<'b> = LineString<'a, D> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        // TODO: pass through field information from array
        match D {
            2 => crate::geo_traits::Dimensions::Xy,
            3 => crate::geo_traits::Dimensions::Xyz,
            _ => todo!(),
        }
    }

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<'a, const D: usize> MultiLineStringTrait for &'a MultiLineString<'a, D> {
    type T = f64;
    type LineStringType<'b> = LineString<'a, D> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        // TODO: pass through field information from array
        match D {
            2 => crate::geo_traits::Dimensions::Xy,
            3 => crate::geo_traits::Dimensions::Xyz,
            _ => todo!(),
        }
    }

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        LineString::new(self.coords, self.ring_offsets, self.start_offset + i)
    }
}

impl<const D: usize> From<MultiLineString<'_, D>> for geo::MultiLineString {
    fn from(value: MultiLineString<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&MultiLineString<'_, D>> for geo::MultiLineString {
    fn from(value: &MultiLineString<'_, D>) -> Self {
        multi_line_string_to_geo(value)
    }
}

impl<const D: usize> From<MultiLineString<'_, D>> for geo::Geometry {
    fn from(value: MultiLineString<'_, D>) -> Self {
        geo::Geometry::MultiLineString(value.into())
    }
}

impl RTreeObject for MultiLineString<'_, 2> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multilinestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: MultiLineStringTrait<T = f64>> PartialEq<G> for MultiLineString<'_, 2> {
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiLineStringArray;
    use crate::test::multilinestring::{ml0, ml1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiLineStringArray<2> = vec![ml0(), ml1()].as_slice().into();
        let arr2: MultiLineStringArray<2> = vec![ml0(), ml0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
