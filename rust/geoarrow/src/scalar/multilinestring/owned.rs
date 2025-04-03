use crate::algorithm::native::eq::multi_line_string_eq;
use crate::array::{CoordBuffer, MultiLineStringArray};
use crate::scalar::{LineString, MultiLineString};
use arrow_buffer::OffsetBuffer;
use geo_traits::MultiLineStringTrait;
use geoarrow_schema::Dimension;

#[derive(Clone, Debug)]
pub struct OwnedMultiLineString {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    ring_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl OwnedMultiLineString {
    pub fn new(
        coords: CoordBuffer,
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

impl<'a> From<&'a OwnedMultiLineString> for MultiLineString<'a> {
    fn from(value: &'a OwnedMultiLineString) -> Self {
        Self::new(
            &value.coords,
            &value.geom_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl From<OwnedMultiLineString> for geo::MultiLineString {
    fn from(value: OwnedMultiLineString) -> Self {
        let geom = MultiLineString::from(&value);
        geom.into()
    }
}

impl<'a> From<MultiLineString<'a>> for OwnedMultiLineString {
    fn from(value: MultiLineString<'a>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl From<OwnedMultiLineString> for MultiLineStringArray {
    fn from(value: OwnedMultiLineString) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            None,
            Default::default(),
        )
    }
}

impl MultiLineStringTrait for OwnedMultiLineString {
    type T = f64;
    type LineStringType<'b>
        = LineString<'b>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            _ => todo!("XYM and XYZM not supported yet"),
        }
    }

    fn num_line_strings(&self) -> usize {
        MultiLineString::from(self).num_line_strings()
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        MultiLineString::from(self).line_string_unchecked(i)
    }
}

impl<G: MultiLineStringTrait<T = f64>> PartialEq<G> for OwnedMultiLineString {
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}
