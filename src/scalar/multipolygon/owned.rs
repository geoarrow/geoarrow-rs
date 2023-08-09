use crate::array::CoordBuffer;
use crate::scalar::MultiPolygon;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;

pub struct OwnedMultiPolygon<O: Offset> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetsBuffer<O>,

    polygon_offsets: OffsetsBuffer<O>,

    ring_offsets: OffsetsBuffer<O>,

    geom_index: usize,
}

impl<O: Offset> OwnedMultiPolygon<O> {
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        polygon_offsets: OffsetsBuffer<O>,
        ring_offsets: OffsetsBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            geom_index,
        }
    }
}

impl<'a, O: Offset> From<OwnedMultiPolygon<O>> for MultiPolygon<'a, O> {
    fn from(value: OwnedMultiPolygon<O>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.polygon_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<'a, O: Offset> From<MultiPolygon<'a, O>> for OwnedMultiPolygon<O> {
    fn from(value: MultiPolygon<'a, O>) -> Self {
        let (coords, geom_offsets, polygon_offsets, ring_offsets, geom_index) =
            value.into_owned_inner();
        Self::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            geom_index,
        )
    }
}
