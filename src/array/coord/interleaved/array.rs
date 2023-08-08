use crate::array::CoordType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::InterleavedCoord;
use crate::GeometryArrayTrait;
use arrow2::array::{Array, FixedSizeListArray, PrimitiveArray};
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};
use rstar::RTree;

/// A an array of XY coordinates stored interleaved in a single buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct InterleavedCoordBuffer {
    pub coords: Buffer<f64>,
}

fn check(coords: &Buffer<f64>) -> Result<()> {
    if coords.len() % 2 != 0 {
        return Err(GeoArrowError::General(
            "x and y arrays must have the same length".to_string(),
        ));
    }

    Ok(())
}

impl InterleavedCoordBuffer {
    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Panics
    ///
    /// - if the coordinate buffer have different lengths
    pub fn new(coords: Buffer<f64>) -> Self {
        check(&coords).unwrap();
        Self { coords }
    }

    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the coordinate buffer have different lengths
    pub fn try_new(coords: Buffer<f64>) -> Result<Self> {
        check(&coords)?;
        Ok(Self { coords })
    }

    pub fn values_array(&self) -> PrimitiveArray<f64> {
        PrimitiveArray::new(DataType::Float64, self.coords.clone(), None)
    }

    pub fn values_field(&self) -> Field {
        Field::new("xy", DataType::Float64, false)
    }
}

impl<'a> GeometryArrayTrait<'a> for InterleavedCoordBuffer {
    type ArrowArray = FixedSizeListArray;
    type Scalar = InterleavedCoord<'a>;
    type ScalarGeo = geo::Coord;
    type RTreeObject = Self::Scalar;

    fn value(&'a self, i: usize) -> Self::Scalar {
        InterleavedCoord {
            coords: &self.coords,
            i,
        }
    }

    fn logical_type(&self) -> DataType {
        DataType::FixedSizeList(Box::new(self.values_field()), 2)
    }

    fn extension_type(&self) -> DataType {
        panic!("Coordinate arrays do not have an extension name.")
    }

    fn into_arrow(self) -> Self::ArrowArray {
        FixedSizeListArray::new(self.logical_type(), self.values_array().boxed(), None)
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        unimplemented!();
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Interleaved
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        panic!("into_coord_type only implemented on CoordBuffer");
    }

    fn rstar_tree(&'a self) -> RTree<Self::Scalar> {
        panic!("not implemented for coords");
    }

    fn len(&self) -> usize {
        self.coords.len() / 2
    }

    fn validity(&self) -> Option<&arrow2::bitmap::Bitmap> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) };
    }

    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.coords.slice_unchecked(offset * 2, length * 2);
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let mut buffer = self.clone();
        buffer.slice(offset, length);
        Self::new(buffer.coords.as_slice().to_vec().into())
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

impl TryFrom<&FixedSizeListArray> for InterleavedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &FixedSizeListArray) -> std::result::Result<Self, Self::Error> {
        if value.size() != 2 {
            return Err(GeoArrowError::General(
                "Expected this FixedSizeListArray to have size 2".to_string(),
            ));
        }

        let coord_array_values = value
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        Ok(InterleavedCoordBuffer::new(
            coord_array_values.values().clone(),
        ))
    }
}

impl TryFrom<Vec<f64>> for InterleavedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: Vec<f64>) -> std::result::Result<Self, Self::Error> {
        Self::try_new(value.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eq_slicing() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let mut buf1 = InterleavedCoordBuffer::new(coords1.into());
        buf1.slice(1, 1);

        let coords2 = vec![1., 4.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into());

        assert_eq!(buf1, buf2);
    }
}
