use arrow_buffer::OffsetBuffer;

use crate::CoordArray;

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a> {
    coords: &'a CoordArray,

    geom_offsets: &'a OffsetBuffer<i64>,

    geom_index: usize,
}

impl<'a> MultiPoint<'a> {
    pub fn new(
        coords: &'a CoordArray,
        geom_offsets: &'a OffsetBuffer<i64>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}
