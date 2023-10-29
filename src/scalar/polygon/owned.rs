use crate::array::CoordBuffer;
use crate::scalar::Polygon;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

pub struct OwnedPolygon<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedPolygon<O> {
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

impl<'a, O: OffsetSizeTrait> From<OwnedPolygon<O>> for Polygon<'a, O> {
    fn from(value: OwnedPolygon<O>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: OffsetSizeTrait> From<OwnedPolygon<O>> for geo::Polygon {
    fn from(value: OwnedPolygon<O>) -> Self {
        let geom = Polygon::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<Polygon<'a, O>> for OwnedPolygon<O> {
    fn from(value: Polygon<'a, O>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}
