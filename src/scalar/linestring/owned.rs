use crate::algorithm::native::eq::line_string_eq;
use crate::array::{CoordBuffer, LineStringArray};
use crate::geo_traits::LineStringTrait;
use crate::scalar::{LineString, Point};
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedLineString<const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl<const D: usize> OwnedLineString<D> {
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, const D: usize> From<&'a OwnedLineString<D>> for LineString<'a, D> {
    fn from(value: &'a OwnedLineString<D>) -> Self {
        Self::new(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl From<OwnedLineString<2>> for geo::LineString {
    fn from(value: OwnedLineString<2>) -> Self {
        let geom = LineString::from(&value);
        geom.into()
    }
}

impl<'a, const D: usize> From<LineString<'a, D>> for OwnedLineString<D> {
    fn from(value: LineString<'a, D>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl<const D: usize> From<OwnedLineString<D>> for LineStringArray<D> {
    fn from(value: OwnedLineString<D>) -> Self {
        Self::new(value.coords, value.geom_offsets, None, Default::default())
    }
}

impl<const D: usize> LineStringTrait for OwnedLineString<D> {
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

impl<G: LineStringTrait<T = f64>> PartialEq<G> for OwnedLineString<2> {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}
