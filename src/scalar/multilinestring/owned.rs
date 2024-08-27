use crate::algorithm::native::eq::multi_line_string_eq;
use crate::array::{CoordBuffer, MultiLineStringArray};
use crate::geo_traits::MultiLineStringTrait;
use crate::scalar::{LineString, MultiLineString};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedMultiLineString<O: OffsetSizeTrait, const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait, const D: usize> OwnedMultiLineString<O, D> {
    pub fn new(
        coords: CoordBuffer<D>,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
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

impl<'a, O: OffsetSizeTrait, const D: usize> From<&'a OwnedMultiLineString<O, D>>
    for MultiLineString<'a, O, D>
{
    fn from(value: &'a OwnedMultiLineString<O, D>) -> Self {
        Self::new(
            &value.coords,
            &value.geom_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: OffsetSizeTrait> From<OwnedMultiLineString<O, 2>> for geo::MultiLineString {
    fn from(value: OwnedMultiLineString<O, 2>) -> Self {
        let geom = MultiLineString::from(&value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<MultiLineString<'a, O, D>>
    for OwnedMultiLineString<O, D>
{
    fn from(value: MultiLineString<'a, O, D>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<OwnedMultiLineString<O, D>>
    for MultiLineStringArray<O, D>
{
    fn from(value: OwnedMultiLineString<O, D>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            None,
            Default::default(),
        )
    }
}

impl<O: OffsetSizeTrait, const D: usize> MultiLineStringTrait for OwnedMultiLineString<O, D> {
    type T = f64;
    type ItemType<'b> = LineString<'b, O, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_lines(&self) -> usize {
        MultiLineString::from(self).num_lines()
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        MultiLineString::from(self).line_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> PartialEq<G>
    for OwnedMultiLineString<O, 2>
{
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}
