use crate::CoordArray;
use arrow_buffer::NullBuffer;

/// An array of point objects
#[derive(Debug, Clone)]
pub struct PointArray {
    /// Coordinate array
    coords: CoordArray,

    /// Null array
    nulls: Option<NullBuffer>,
}

impl PointArray {
    pub fn new(coords: CoordArray, nulls: Option<NullBuffer>) -> Self {
        Self { coords, nulls }
    }
}
