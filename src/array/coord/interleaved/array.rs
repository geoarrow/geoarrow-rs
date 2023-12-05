use std::sync::Arc;

use crate::array::{CoordType, InterleavedCoordBufferBuilder};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::CoordTrait;
use crate::scalar::InterleavedCoord;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::GeometryArrayTrait;
use arrow_array::{Array, FixedSizeListArray, Float64Array};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

/// A an array of XY coordinates stored interleaved in a single buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct InterleavedCoordBuffer {
    pub coords: ScalarBuffer<f64>,
}

fn check(coords: &ScalarBuffer<f64>) -> Result<()> {
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
    pub fn new(coords: ScalarBuffer<f64>) -> Self {
        check(&coords).unwrap();
        Self { coords }
    }

    /// Construct a new InterleavedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the coordinate buffer have different lengths
    pub fn try_new(coords: ScalarBuffer<f64>) -> Result<Self> {
        check(&coords)?;
        Ok(Self { coords })
    }

    pub fn values_array(&self) -> Float64Array {
        Float64Array::new(self.coords.clone(), None)
    }

    pub fn values_field(&self) -> Field {
        Field::new("xy", DataType::Float64, false)
    }
}

impl GeometryArrayTrait for InterleavedCoordBuffer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &crate::datatypes::GeoDataType {
        panic!("Coordinate arrays do not have a GeoDataType.")
    }

    fn storage_type(&self) -> DataType {
        DataType::FixedSizeList(Arc::new(self.values_field()), 2)
    }

    fn extension_field(&self) -> Arc<Field> {
        panic!("Coordinate arrays do not have an extension name.")
    }

    fn extension_name(&self) -> &str {
        panic!("Coordinate arrays do not have an extension name.")
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Interleaved
    }

    fn len(&self) -> usize {
        self.coords.len() / 2
    }

    fn validity(&self) -> Option<&NullBuffer> {
        panic!("coordinate arrays don't have their own validity arrays")
    }
}

impl GeometryArraySelfMethods for InterleavedCoordBuffer {
    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        unimplemented!();
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        panic!("into_coord_type only implemented on CoordBuffer");
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            coords: self.coords.slice(offset * 2, length * 2),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let buffer = self.slice(offset, length);
        Self::new(buffer.coords.to_vec().into())
    }
}

impl<'a> GeometryArrayAccessor<'a> for InterleavedCoordBuffer {
    type Item = InterleavedCoord<'a>;
    type ItemGeo = geo::Coord;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        InterleavedCoord {
            coords: &self.coords,
            i: index,
        }
    }
}

impl IntoArrow for InterleavedCoordBuffer {
    type ArrowArray = FixedSizeListArray;

    fn into_arrow(self) -> Self::ArrowArray {
        FixedSizeListArray::new(
            Arc::new(self.values_field()),
            2,
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

impl TryFrom<&FixedSizeListArray> for InterleavedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &FixedSizeListArray) -> std::result::Result<Self, Self::Error> {
        if value.value_length() != 2 {
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

impl TryFrom<Vec<f64>> for InterleavedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: Vec<f64>) -> std::result::Result<Self, Self::Error> {
        Self::try_new(value.into())
    }
}

impl<G: CoordTrait<T = f64>> From<&[G]> for InterleavedCoordBuffer {
    fn from(other: &[G]) -> Self {
        let mut_arr: InterleavedCoordBufferBuilder = other.into();
        mut_arr.into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eq_slicing() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::new(coords1.into()).slice(1, 1);

        let coords2 = vec![1., 4.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into());

        assert_eq!(buf1, buf2);
    }
}
