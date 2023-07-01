use arrow2::array::Array;

use crate::{Coord, GeometryArrayTrait, InterleavedCoordArray, SeparatedCoordArray};

#[derive(Debug, Clone)]
pub enum CoordArray {
    Interleaved(InterleavedCoordArray),
    Separated(SeparatedCoordArray),
}

impl CoordArray {
    pub fn get_x(&self, i: usize) -> f64 {
        // NOTE: for interleaved this needs to be i*2 so it accesses the right point
        todo!();
    }

    pub fn get_y(&self, i: usize) -> f64 {
        todo!();
    }

    pub fn len(&self) -> usize {
        todo!()
    }
}

impl<'a> GeometryArrayTrait<'a> for CoordArray {
    type ArrowArray = Box<dyn Array>;
    type Scalar = Coord<'a>;
    type ScalarGeo = geo::Coord;

    fn value(&'a self, i: usize) -> Self::Scalar {
        match self {
            CoordArray::Interleaved(c) => Coord::Interleaved(c.value(i)),
            CoordArray::Separated(c) => Coord::Separated(c.value(i)),
        }
    }

    fn into_arrow(self) -> Self::ArrowArray {
        match self {
            CoordArray::Interleaved(c) => c.into_arrow().boxed(),
            CoordArray::Separated(c) => c.into_arrow().boxed(),
        }
    }

    fn len(&self) -> usize {
        match self {
            CoordArray::Interleaved(c) => c.len(),
            CoordArray::Separated(c) => c.len(),
        }
    }

    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        match self {
            CoordArray::Interleaved(c) => CoordArray::Interleaved(c.slice(offset, length)),
            CoordArray::Separated(c) => CoordArray::Separated(c.slice(offset, length)),
        }
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        match self {
            CoordArray::Interleaved(c) => {
                CoordArray::Interleaved(c.slice_unchecked(offset, length))
            }
            CoordArray::Separated(c) => CoordArray::Separated(c.slice_unchecked(offset, length)),
        }
    }

    fn to_boxed(&self) -> Box<Self> {
        match self {
            CoordArray::Interleaved(c) => self.to_boxed(),
            CoordArray::Separated(c) => self.to_boxed(),
        }
    }
}
