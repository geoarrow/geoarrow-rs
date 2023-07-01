use crate::{CoordArray, GeometryArrayTrait};
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

impl<'a> GeometryArrayTrait<'a> for PointArray {
    type Scalar = crate::Point<'a>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        crate::Point::new(&self.coords, i)
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }
}
