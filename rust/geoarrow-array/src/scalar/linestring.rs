use arrow_buffer::OffsetBuffer;
use geo_traits::LineStringTrait;

use crate::array::CoordBuffer;
use crate::eq::line_string_eq;
use crate::scalar::Coord;
use crate::util::OffsetBufferUtils;

/// An Arrow equivalent of a LineString
///
/// This implements [LineStringTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct LineString<'a> {
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> LineString<'a> {
    pub(crate) fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            geom_index,
            start_offset,
        }
    }

    pub(crate) fn into_owned_inner(self) -> (CoordBuffer, OffsetBuffer<i32>, usize) {
        (
            self.coords.clone(),
            self.geom_offsets.clone(),
            self.geom_index,
        )
    }
}

impl<'a> LineStringTrait for LineString<'a> {
    type T = f64;
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.coords.value(self.start_offset + i)
    }
}

impl<'a> LineStringTrait for &'a LineString<'a> {
    type T = f64;
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.coords.value(self.start_offset + i)
    }
}

impl<G: LineStringTrait<T = f64>> PartialEq<G> for LineString<'_> {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}
