use crate::array::CoordBuffer;
use crate::scalar::MultiPoint;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

pub struct OwnedMultiPoint<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedMultiPoint<O> {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait> From<OwnedMultiPoint<O>> for MultiPoint<'a, O> {
    fn from(value: OwnedMultiPoint<O>) -> Self {
        Self::new_owned(value.coords, value.geom_offsets, value.geom_index)
    }
}

impl<O: OffsetSizeTrait> From<OwnedMultiPoint<O>> for geo::MultiPoint {
    fn from(value: OwnedMultiPoint<O>) -> Self {
        let geom = MultiPoint::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<MultiPoint<'a, O>> for OwnedMultiPoint<O> {
    fn from(value: MultiPoint<'a, O>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}
