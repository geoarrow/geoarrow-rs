use crate::array::CoordBuffer;
use crate::scalar::MultiLineString;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;

pub struct OwnedMultiLineString<O: Offset> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetsBuffer<O>,

    ring_offsets: OffsetsBuffer<O>,

    geom_index: usize,
}

impl<O: Offset> OwnedMultiLineString<O> {
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        ring_offsets: OffsetsBuffer<O>,
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

impl<'a, O: Offset> From<OwnedMultiLineString<O>> for MultiLineString<'a, O> {
    fn from(value: OwnedMultiLineString<O>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<'a, O: Offset> From<MultiLineString<'a, O>> for OwnedMultiLineString<O> {
    fn from(value: MultiLineString<'a, O>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}
