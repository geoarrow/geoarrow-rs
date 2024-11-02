use std::sync::Arc;

use crate::array::{CoordType, InterleavedCoordBufferBuilder};
use crate::datatypes::{coord_type_to_data_type, Dimension};
use crate::error::{GeoArrowError, Result};
use crate::scalar::InterleavedCoord;
use crate::trait_::IntoArrow;
use arrow_array::{Array, FixedSizeListArray, Float64Array};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field};
use geo_traits::CoordTrait;

/// A an array of coordinates stored interleaved in a single buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct InterleavedCoordBuffer {
    pub(crate) coords: ScalarBuffer<f64>,
    pub(crate) dim: Dimension,
}

fn check(coords: &ScalarBuffer<f64>, dim: Dimension) -> Result<()> {
    if coords.len() % dim.size() != 0 {
        return Err(GeoArrowError::General(
            "Length of interleaved coordinate buffer must be a multiple of the dimension size"
                .to_string(),
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
    pub fn new(coords: ScalarBuffer<f64>, dim: Dimension) -> Self {
        Self::try_new(coords, dim).unwrap()
    }

    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the coordinate buffer have different lengths
    pub fn try_new(coords: ScalarBuffer<f64>, dim: Dimension) -> Result<Self> {
        check(&coords, dim)?;
        Ok(Self { coords, dim })
    }

    pub fn from_vec(coords: Vec<f64>, dim: Dimension) -> Result<Self> {
        Self::try_new(coords.into(), dim)
    }

    pub fn from_coords<G: CoordTrait<T = f64>>(coords: &[G], dim: Dimension) -> Result<Self> {
        Ok(InterleavedCoordBufferBuilder::from_coords(coords, dim)?.into())
    }

    /// Access the underlying coordinate buffer.
    pub fn coords(&self) -> &ScalarBuffer<f64> {
        &self.coords
    }

    pub fn values_array(&self) -> Float64Array {
        Float64Array::new(self.coords.clone(), None)
    }

    pub fn dim(&self) -> Dimension {
        self.dim
    }

    pub fn values_field(&self) -> Field {
        match self.dim {
            Dimension::XY => Field::new("xy", DataType::Float64, false),
            Dimension::XYZ => Field::new("xyz", DataType::Float64, false),
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            coords: self
                .coords
                .slice(offset * self.dim.size(), length * self.dim.size()),
            dim: self.dim,
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let buffer = self.slice(offset, length);
        Self::new(buffer.coords.to_vec().into(), self.dim)
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    pub fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    pub fn storage_type(&self) -> DataType {
        coord_type_to_data_type(CoordType::Interleaved, self.dim)
    }

    // todo switch to:
    // pub const coord_type: CoordType = CoordType::Interleaved;

    pub fn coord_type(&self) -> CoordType {
        CoordType::Interleaved
    }

    pub fn len(&self) -> usize {
        self.coords.len() / self.dim.size()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, index: usize) -> InterleavedCoord<'_> {
        assert!(index <= self.len());
        self.value_unchecked(index)
    }

    pub fn value_unchecked(&self, index: usize) -> InterleavedCoord<'_> {
        InterleavedCoord {
            coords: &self.coords,
            i: index,
            dim: self.dim,
        }
    }

    pub fn from_arrow(array: &FixedSizeListArray, dim: Dimension) -> Result<Self> {
        if array.value_length() != dim.size() as i32 {
            return Err(GeoArrowError::General(
                "Expected this FixedSizeListArray to have size 2".to_string(),
            ));
        }

        let coord_array_values = array
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        Ok(InterleavedCoordBuffer::new(
            coord_array_values.values().clone(),
            dim,
        ))
    }
}

impl IntoArrow for InterleavedCoordBuffer {
    type ArrowArray = FixedSizeListArray;

    fn into_arrow(self) -> Self::ArrowArray {
        FixedSizeListArray::new(
            Arc::new(self.values_field()),
            self.dim.size() as i32,
            Arc::new(self.values_array()),
            None,
        )
    }
}

impl From<InterleavedCoordBuffer> for FixedSizeListArray {
    fn from(value: InterleavedCoordBuffer) -> Self {
        value.into_arrow()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eq_slicing() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::new(coords1.into(), Dimension::XY).slice(1, 1);

        let coords2 = vec![1., 4.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into(), Dimension::XY);

        assert_eq!(buf1, buf2);
    }
}
