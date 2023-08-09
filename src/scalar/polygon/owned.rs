use crate::array::CoordBuffer;
use crate::scalar::Polygon;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;

pub struct OwnedPolygon<O: Offset> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetsBuffer<O>,

    ring_offsets: OffsetsBuffer<O>,

    geom_index: usize,
}

impl<O: Offset> OwnedPolygon<O> {
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

impl<'a, O: Offset> From<OwnedPolygon<O>> for Polygon<'a, O> {
    fn from(value: OwnedPolygon<O>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: Offset> From<OwnedPolygon<O>> for geo::Polygon {
    fn from(value: OwnedPolygon<O>) -> Self {
        let geom = Polygon::from(value);
        geom.into()
    }
}

impl<'a, O: Offset> From<Polygon<'a, O>> for OwnedPolygon<O> {
    fn from(value: Polygon<'a, O>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}
