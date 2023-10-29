use crate::array::CoordBuffer;
use crate::scalar::MultiLineString;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

pub struct OwnedMultiLineString<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedMultiLineString<O> {
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
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

impl<'a, O: OffsetSizeTrait> From<OwnedMultiLineString<O>> for MultiLineString<'a, O> {
    fn from(value: OwnedMultiLineString<O>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: OffsetSizeTrait> From<OwnedMultiLineString<O>> for geo::MultiLineString {
    fn from(value: OwnedMultiLineString<O>) -> Self {
        let geom = MultiLineString::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<MultiLineString<'a, O>> for OwnedMultiLineString<O> {
    fn from(value: MultiLineString<'a, O>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}
