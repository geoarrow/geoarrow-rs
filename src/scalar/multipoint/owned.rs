use crate::array::CoordBuffer;
use crate::scalar::MultiPoint;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;

pub struct OwnedMultiPoint<O: Offset> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetsBuffer<O>,

    geom_index: usize,
}

impl<O: Offset> OwnedMultiPoint<O> {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetsBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: Offset> From<OwnedMultiPoint<O>> for MultiPoint<'a, O> {
    fn from(value: OwnedMultiPoint<O>) -> Self {
        Self::new_owned(value.coords, value.geom_offsets, value.geom_index)
    }
}

impl<'a, O: Offset> From<MultiPoint<'a, O>> for OwnedMultiPoint<O> {
    fn from(value: MultiPoint<'a, O>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}
