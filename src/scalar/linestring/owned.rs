use crate::array::CoordBuffer;
use crate::scalar::LineString;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

pub struct OwnedLineString<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedLineString<O> {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait> From<OwnedLineString<O>> for LineString<'a, O> {
    fn from(value: OwnedLineString<O>) -> Self {
        Self::new_owned(value.coords, value.geom_offsets, value.geom_index)
    }
}

impl<O: OffsetSizeTrait> From<OwnedLineString<O>> for geo::LineString {
    fn from(value: OwnedLineString<O>) -> Self {
        let geom = LineString::from(value);
        geom.into()
    }
}
impl<'a, O: OffsetSizeTrait> From<LineString<'a, O>> for OwnedLineString<O> {
    fn from(value: LineString<'a, O>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}
