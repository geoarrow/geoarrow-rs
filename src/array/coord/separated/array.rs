use std::sync::Arc;

// use arrow2::array::{Array, PrimitiveArray, StructArray};
use arrow_array::{Array, Float64Array, StructArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

use crate::array::{CoordType, SeparatedCoordBufferBuilder};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::CoordTrait;
use crate::scalar::SeparatedCoord;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::GeometryArrayTrait;

#[derive(Debug, Clone, PartialEq)]
pub struct SeparatedCoordBuffer {
    pub x: ScalarBuffer<f64>,
    pub y: ScalarBuffer<f64>,
}

fn check(x: &ScalarBuffer<f64>, y: &ScalarBuffer<f64>) -> Result<()> {
    if x.len() != y.len() {
        return Err(GeoArrowError::General(
            "x and y arrays must have the same length".to_string(),
        ));
    }

    Ok(())
}

impl SeparatedCoordBuffer {
    /// Construct a new SeparatedCoordBuffer
    ///
    /// # Panics
    ///
    /// - if the x and y buffers have different lengths
    pub fn new(x: ScalarBuffer<f64>, y: ScalarBuffer<f64>) -> Self {
        check(&x, &y).unwrap();
        Self { x, y }
    }

    /// Construct a new SeparatedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the x and y buffers have different lengths
    pub fn try_new(x: ScalarBuffer<f64>, y: ScalarBuffer<f64>) -> Result<Self> {
        check(&x, &y)?;
        Ok(Self { x, y })
    }

    pub fn values_array(&self) -> Vec<Arc<dyn Array>> {
        vec![
            Arc::new(Float64Array::new(self.x.clone(), None)),
            Arc::new(Float64Array::new(self.y.clone(), None)),
        ]
    }

    pub fn values_field(&self) -> Vec<Field> {
        vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]
    }
}

impl GeometryArrayTrait for SeparatedCoordBuffer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &crate::datatypes::GeoDataType {
        panic!("Coordinate arrays do not have a GeoDataType.")
    }

    fn storage_type(&self) -> DataType {
        DataType::Struct(self.values_field().into())
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
        CoordType::Separated
    }

    fn len(&self) -> usize {
        self.x.len()
    }

    fn validity(&self) -> Option<&NullBuffer> {
        panic!("coordinate arrays don't have their own validity arrays")
    }
}

impl GeometryArraySelfMethods for SeparatedCoordBuffer {
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
            x: self.x.slice(offset, length),
            y: self.y.slice(offset, length),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let buffer = self.slice(offset, length);
        Self::new(buffer.x.to_vec().into(), buffer.y.to_vec().into())
    }
}

impl<'a> GeometryArrayAccessor<'a> for SeparatedCoordBuffer {
    type Item = SeparatedCoord<'a>;
    type ItemGeo = geo::Coord;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        SeparatedCoord {
            x: &self.x,
            y: &self.y,
            i: index,
        }
    }
}

impl IntoArrow for SeparatedCoordBuffer {
    type ArrowArray = StructArray;

    fn into_arrow(self) -> Self::ArrowArray {
        StructArray::new(self.values_field().into(), self.values_array(), None)
    }
}

impl From<SeparatedCoordBuffer> for StructArray {
    fn from(value: SeparatedCoordBuffer) -> Self {
        value.into_arrow()
    }
}

impl TryFrom<&StructArray> for SeparatedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &StructArray) -> Result<Self> {
        let arrays = value.columns();

        if !arrays.len() == 2 {
            return Err(GeoArrowError::General(
                "Expected two child arrays of this StructArray.".to_string(),
            ));
        }

        let x_array_values = arrays[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let y_array_values = arrays[1].as_any().downcast_ref::<Float64Array>().unwrap();

        Ok(SeparatedCoordBuffer::new(
            x_array_values.values().clone(),
            y_array_values.values().clone(),
        ))
    }
}

impl TryFrom<(Vec<f64>, Vec<f64>)> for SeparatedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: (Vec<f64>, Vec<f64>)) -> std::result::Result<Self, Self::Error> {
        Self::try_new(value.0.into(), value.1.into())
    }
}

impl<G: CoordTrait<T = f64>> From<&[G]> for SeparatedCoordBuffer {
    fn from(other: &[G]) -> Self {
        let mut_arr: SeparatedCoordBufferBuilder = other.into();
        mut_arr.into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eq_slicing() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let buf1 = SeparatedCoordBuffer::new(x1.into(), y1.into()).slice(1, 1);
        dbg!(&buf1.x);
        dbg!(&buf1.y);

        let x2 = vec![1.];
        let y2 = vec![4.];
        let buf2 = SeparatedCoordBuffer::new(x2.into(), y2.into());

        assert_eq!(buf1, buf2);
    }
}
