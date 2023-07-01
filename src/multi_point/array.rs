use arrow_array::ListArray;
use arrow_buffer::{NullBuffer, OffsetBuffer};

use crate::{CoordArray, GeometryArrayTrait};

/// A [`GeometryArrayTrait`] semantically equivalent to `Vec<Option<MultiPoint>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct MultiPointArray {
    coords: CoordArray,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i64>,

    /// Null array
    nulls: Option<NullBuffer>,
}

impl MultiPointArray {
    pub fn new(
        coords: CoordArray,
        geom_offsets: OffsetBuffer<i64>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            nulls,
        }
    }
}


impl<'a> GeometryArrayTrait<'a> for MultiPointArray {
    type Scalar = crate::MultiPoint<'a>;


}
