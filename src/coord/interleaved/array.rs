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

    fn into_arrow(self) -> Self::ArrowArray {
        todo!();
    }

    fn len(&self) -> usize {
        self.coords.len() / 2
    }

    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        InterleavedCoordArray::new(self.coords.slice(offset * 2, length * 2))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let new_coords = unsafe { self.coords.slice_unchecked(offset * 2, length * 2) };
        InterleavedCoordArray { coords: new_coords }
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}
