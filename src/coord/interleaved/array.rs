use arrow2::array::FixedSizeListArray;
use arrow2::buffer::Buffer;

use crate::{GeometryArrayTrait, InterleavedCoord};

/// A an array of XY coordinates stored interleaved in a single buffer.
#[derive(Debug, Clone)]
pub struct InterleavedCoordArray {
    coords: Buffer<f64>,
}

impl InterleavedCoordArray {
    pub fn new(coords: Buffer<f64>) -> Self {
        Self { coords }
    }
}

impl<'a> GeometryArrayTrait<'a> for InterleavedCoordArray {
    type ArrowArray = FixedSizeListArray;
    type Scalar = InterleavedCoord<'a>;
    type ScalarGeo = geo::Coord;

    fn value(&'a self, i: usize) -> Self::Scalar {
        InterleavedCoord {
            coords: &self.coords,
            i,
        }
    }
}
