use crate::array::CoordBuffer;
use crate::scalar::LineString;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;

pub struct OwnedLineString<O: Offset> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetsBuffer<O>,

    geom_index: usize,
}

impl<O: Offset> OwnedLineString<O> {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetsBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: Offset> From<OwnedLineString<O>> for LineString<'a, O> {
    fn from(value: OwnedLineString<O>) -> Self {
        Self::new_owned(value.coords, value.geom_offsets, value.geom_index)
    }
}

impl<'a, O: Offset> From<LineString<'a, O>> for OwnedLineString<O> {
    fn from(value: LineString<'a, O>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}
