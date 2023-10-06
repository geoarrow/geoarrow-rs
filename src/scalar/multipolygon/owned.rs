use crate::array::CoordBuffer;
use crate::scalar::MultiPolygon;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

pub struct OwnedMultiPolygon<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    polygon_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedMultiPolygon<O> {
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        polygon_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
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

impl<'a, O: OffsetSizeTrait> From<OwnedMultiPolygon<O>> for MultiPolygon<'a, O> {
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

impl<O: OffsetSizeTrait> From<OwnedMultiPolygon<O>> for geo::MultiPolygon {
    fn from(value: OwnedMultiPolygon<O>) -> Self {
        let geom = MultiPolygon::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<MultiPolygon<'a, O>> for OwnedMultiPolygon<O> {
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
