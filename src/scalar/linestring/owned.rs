use crate::algorithm::native::eq::line_string_eq;
use crate::array::CoordBuffer;
use crate::geo_traits::LineStringTrait;
use crate::scalar::{LineString, Point};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Debug)]
pub struct OwnedLineString<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedLineString<O> {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait> From<OwnedLineString<O>> for LineString<'a, O> {
    fn from(value: OwnedLineString<O>) -> Self {
        Self::new_owned(value.coords, value.geom_offsets, value.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedLineString<O>> for LineString<'a, O> {
    fn from(value: &'a OwnedLineString<O>) -> Self {
        Self::new_borrowed(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl<O: OffsetSizeTrait> From<OwnedLineString<O>> for geo::LineString {
    fn from(value: OwnedLineString<O>) -> Self {
        let geom = LineString::from(value);
        geom.into()
    }
}
impl<'a, O: OffsetSizeTrait> From<LineString<'a, O>> for OwnedLineString<O> {
    fn from(value: LineString<'a, O>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl<O: OffsetSizeTrait> LineStringTrait for OwnedLineString<O> {
    type T = f64;
    type ItemType<'b> = Point<'b> where Self: 'b;

    fn num_coords(&self) -> usize {
        LineString::from(self).num_coords()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString::from(self).coord_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> PartialEq<G> for OwnedLineString<O> {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}
