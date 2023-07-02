use arrow2::array::{FixedSizeListArray, PrimitiveArray, Array};
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};

use crate::error::GeoArrowError;
use crate::{GeometryArrayTrait, InterleavedCoord};

/// A an array of XY coordinates stored interleaved in a single buffer.
#[derive(Debug, Clone)]
pub struct InterleavedCoordBuffer {
    coords: Buffer<f64>,
}

impl InterleavedCoordBuffer {
    pub fn new(coords: Buffer<f64>) -> Self {
        Self { coords }
    }

    pub fn data_type(&self) -> DataType {
        DataType::FixedSizeList(Box::new(self.values_field()), 2)
    }

    pub fn values_array(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::new(DataType::Float64, self.coords, None)
    }

    pub fn values_field(&self) -> Field {
        Field::new("", DataType::Float64, false)
    }
}

impl<'a> GeometryArrayTrait<'a> for InterleavedCoordBuffer {
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
        FixedSizeListArray::new(
            self.data_type(),
            self.values_array().boxed(),
            None,
        )
    }

    fn len(&self) -> usize {
        self.coords.len() / 2
    }

    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        InterleavedCoordBuffer::new(self.coords.slice(offset * 2, length * 2))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let new_coords = unsafe { self.coords.slice_unchecked(offset * 2, length * 2) };
        InterleavedCoordBuffer { coords: new_coords }
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}


impl From<InterleavedCoordBuffer> for FixedSizeListArray {
    fn from(value: InterleavedCoordBuffer) -> Self {
        value.into_arrow()
    }
}

impl TryFrom<FixedSizeListArray> for InterleavedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: FixedSizeListArray) -> Result<Self, Self::Error> {
        todo!()
    }
}
