use crate::algorithm::native::eq::line_string_eq;
use crate::array::{CoordBuffer, LineStringArray};
use crate::geo_traits::LineStringTrait;
use crate::scalar::{LineString, Point};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedLineString<O: OffsetSizeTrait, const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait, const D: usize> OwnedLineString<O, D> {
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<&'a OwnedLineString<O, D>>
    for LineString<'a, O, D>
{
    fn from(value: &'a OwnedLineString<O, D>) -> Self {
        Self::new(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl<O: OffsetSizeTrait> From<OwnedLineString<O, 2>> for geo::LineString {
    fn from(value: OwnedLineString<O, 2>) -> Self {
        let geom = LineString::from(&value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<LineString<'a, O, D>> for OwnedLineString<O, D> {
    fn from(value: LineString<'a, O, D>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<OwnedLineString<O, D>> for LineStringArray<O, D> {
    fn from(value: OwnedLineString<O, D>) -> Self {
        Self::new(value.coords, value.geom_offsets, None, Default::default())
    }
}

impl<O: OffsetSizeTrait, const D: usize> LineStringTrait for OwnedLineString<O, D> {
    type T = f64;
    type ItemType<'b> = Point<'b, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_coords(&self) -> usize {
        LineString::from(self).num_coords()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString::from(self).coord_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> PartialEq<G> for OwnedLineString<O, 2> {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}
