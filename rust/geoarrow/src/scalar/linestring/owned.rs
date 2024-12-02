use crate::algorithm::native::eq::line_string_eq;
use crate::array::{CoordBuffer, LineStringArray};
use crate::scalar::{Coord, LineString};
use arrow_buffer::OffsetBuffer;
use geo_traits::LineStringTrait;

#[derive(Clone, Debug)]
pub struct OwnedLineString {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl OwnedLineString {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetBuffer<i32>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a> From<&'a OwnedLineString> for LineString<'a> {
    fn from(value: &'a OwnedLineString) -> Self {
        Self::new(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl From<OwnedLineString> for geo::LineString {
    fn from(value: OwnedLineString) -> Self {
        let geom = LineString::from(&value);
        geom.into()
    }
}

impl<'a> From<LineString<'a>> for OwnedLineString {
    fn from(value: LineString<'a>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl From<OwnedLineString> for LineStringArray {
    fn from(value: OwnedLineString) -> Self {
        Self::new(value.coords, value.geom_offsets, None, Default::default())
    }
}

impl LineStringTrait for OwnedLineString {
    type T = f64;
    type CoordType<'b>
        = Coord<'b>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn num_coords(&self) -> usize {
        LineString::from(self).num_coords()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        LineString::from(self).coord_unchecked(i)
    }
}

impl<G: LineStringTrait<T = f64>> PartialEq<G> for OwnedLineString {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}
