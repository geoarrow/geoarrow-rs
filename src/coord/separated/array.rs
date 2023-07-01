use arrow2::array::StructArray;
use arrow2::buffer::Buffer;

use crate::{GeometryArrayTrait, SeparatedCoord};

#[derive(Debug, Clone)]
pub struct SeparatedCoordArray {
    x: Buffer<f64>,
    y: Buffer<f64>,
}

impl SeparatedCoordArray {
    pub fn new(x: Buffer<f64>, y: Buffer<f64>) -> Self {
        Self { x, y }
    }
}

impl<'a> GeometryArrayTrait<'a> for SeparatedCoordArray {
    type ArrowArray = StructArray;
    type Scalar = SeparatedCoord<'a>;
    type ScalarGeo = geo::Coord;

    fn value(&'a self, i: usize) -> Self::Scalar {
        SeparatedCoord {
            x: &self.x,
            y: &self.y,
            i,
        }
    }

    fn into_arrow(self) -> Self::ArrowArray {
        todo!();
    }

    fn len(&self) -> usize {
        self.x.len()
    }

    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        SeparatedCoordArray::new(self.x.slice(offset, length), self.y.slice(offset, length))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let (new_x, new_y) = unsafe {
            (
                self.x.slice_unchecked(offset, length),
                self.y.slice_unchecked(offset, length),
            )
        };
        SeparatedCoordArray { x: new_x, y: new_y }
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}
