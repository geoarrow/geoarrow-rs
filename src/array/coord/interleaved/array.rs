use core::panic;
use std::sync::Arc;

use crate::array::{CoordType, InterleavedCoordBufferBuilder};
use crate::datatypes::coord_type_to_data_type;
use crate::error::{GeoArrowError, Result};
use crate::scalar::InterleavedCoord;
use crate::trait_::IntoArrow;
use arrow_array::{Array, FixedSizeListArray, Float64Array};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use geo_traits::{CoordTrait, PointTrait};

/// A an array of XY coordinates stored interleaved in a single buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct InterleavedCoordBuffer<const D: usize> {
    pub(crate) coords: ScalarBuffer<f64>,
}

fn check<const D: usize>(coords: &ScalarBuffer<f64>) -> Result<()> {
    if coords.len() % D != 0 {
        return Err(GeoArrowError::General(
            "x and y arrays must have the same length".to_string(),
        ));
    }

    Ok(())
}

impl<const D: usize> InterleavedCoordBuffer<D> {
    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Panics
    ///
    /// - if the coordinate buffer have different lengths
    pub fn new(coords: ScalarBuffer<f64>) -> Self {
        check::<D>(&coords).unwrap();
        Self { coords }
    }

    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the coordinate buffer have different lengths
    pub fn try_new(coords: ScalarBuffer<f64>) -> Result<Self> {
        check::<D>(&coords)?;
        Ok(Self { coords })
    }

    /// Access the underlying coordinate buffer.
    pub fn coords(&self) -> &ScalarBuffer<f64> {
        &self.coords
    }

    pub fn values_array(&self) -> Float64Array {
        Float64Array::new(self.coords.clone(), None)
    }

    pub fn values_field(&self) -> Field {
        match D {
            2 => Field::new("xy", DataType::Float64, false),
            3 => Field::new("xyz", DataType::Float64, false),
            _ => panic!(),
        }
    }

    pub fn get_x(&self, i: usize) -> f64 {
        let c = self.value(i);
        c.x()
    }

    pub fn get_y(&self, i: usize) -> f64 {
        let c = self.value(i);
        c.y()
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            coords: self.coords.slice(offset * D, length * D),
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let buffer = self.slice(offset, length);
        Self::new(buffer.coords.to_vec().into())
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    pub fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    pub fn storage_type(&self) -> DataType {
        coord_type_to_data_type(CoordType::Interleaved, D.try_into().unwrap())
    }

    // todo switch to:
    // pub const coord_type: CoordType = CoordType::Interleaved;

    pub fn coord_type(&self) -> CoordType {
        CoordType::Interleaved
    }

    pub fn len(&self) -> usize {
        self.coords.len() / D
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, index: usize) -> InterleavedCoord<'_, D> {
        assert!(index <= self.len());
        self.value_unchecked(index)
    }

    pub fn value_unchecked(&self, index: usize) -> InterleavedCoord<'_, D> {
        InterleavedCoord {
            coords: &self.coords,
            i: index,
        }
    }
}

impl<const D: usize> IntoArrow for InterleavedCoordBuffer<D> {
    type ArrowArray = FixedSizeListArray;

    fn into_arrow(self) -> Self::ArrowArray {
        FixedSizeListArray::new(
            Arc::new(self.values_field()),
            D as i32,
            Arc::new(self.values_array()),
            None,
        )
    }
}

impl<const D: usize> From<InterleavedCoordBuffer<D>> for FixedSizeListArray {
    fn from(value: InterleavedCoordBuffer<D>) -> Self {
        value.into_arrow()
    }
}

impl<const D: usize> TryFrom<&FixedSizeListArray> for InterleavedCoordBuffer<D> {
    type Error = GeoArrowError;

    fn try_from(value: &FixedSizeListArray) -> std::result::Result<Self, Self::Error> {
        if value.value_length() != D as i32 {
            return Err(GeoArrowError::General(
                "Expected this FixedSizeListArray to have size 2".to_string(),
            ));
        }

        let coord_array_values = value
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        Ok(InterleavedCoordBuffer::new(
            coord_array_values.values().clone(),
        ))
    }
}

impl<const D: usize> TryFrom<Vec<f64>> for InterleavedCoordBuffer<D> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<f64>) -> std::result::Result<Self, Self::Error> {
        Self::try_new(value.into())
    }
}

impl<const D: usize> From<&[f64]> for InterleavedCoordBuffer<D> {
    fn from(value: &[f64]) -> Self {
        InterleavedCoordBuffer {
            coords: Buffer::from_slice_ref(value).into(),
        }
    }
}

impl<G: PointTrait<T = f64>> From<&[G]> for InterleavedCoordBuffer<2> {
    fn from(other: &[G]) -> Self {
        let mut_arr: InterleavedCoordBufferBuilder<2> = other.into();
        mut_arr.into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eq_slicing() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::<2>::new(coords1.into()).slice(1, 1);

        let coords2 = vec![1., 4.];
        let buf2 = InterleavedCoordBuffer::<2>::new(coords2.into());

        assert_eq!(buf1, buf2);
    }
}
