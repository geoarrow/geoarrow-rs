use crate::algorithm::native::eq::multi_line_string_eq;
use crate::array::{CoordBuffer, MultiLineStringArray};
use crate::scalar::{LineString, MultiLineString};
use arrow_buffer::OffsetBuffer;
use geo_traits::MultiLineStringTrait;

#[derive(Clone, Debug)]
pub struct OwnedMultiLineString<const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    ring_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl<const D: usize> OwnedMultiLineString<D> {
    pub fn new(
        coords: CoordBuffer<D>,
        geom_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
        }
    }
}

impl<'a, const D: usize> From<&'a OwnedMultiLineString<D>> for MultiLineString<'a, D> {
    fn from(value: &'a OwnedMultiLineString<D>) -> Self {
        Self::new(
            &value.coords,
            &value.geom_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl From<OwnedMultiLineString<2>> for geo::MultiLineString {
    fn from(value: OwnedMultiLineString<2>) -> Self {
        let geom = MultiLineString::from(&value);
        geom.into()
    }
}

impl<'a, const D: usize> From<MultiLineString<'a, D>> for OwnedMultiLineString<D> {
    fn from(value: MultiLineString<'a, D>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl<const D: usize> From<OwnedMultiLineString<D>> for MultiLineStringArray<D> {
    fn from(value: OwnedMultiLineString<D>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            None,
            Default::default(),
        )
    }
}

impl<const D: usize> MultiLineStringTrait for OwnedMultiLineString<D> {
    type T = f64;
    type LineStringType<'b> = LineString<'b, D> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        // TODO: pass through field information from array
        match D {
            2 => geo_traits::Dimensions::Xy,
            3 => geo_traits::Dimensions::Xyz,
            _ => todo!(),
        }
    }

    fn num_line_strings(&self) -> usize {
        MultiLineString::from(self).num_line_strings()
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        MultiLineString::from(self).line_string_unchecked(i)
    }
}

impl<G: MultiLineStringTrait<T = f64>> PartialEq<G> for OwnedMultiLineString<2> {
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}
