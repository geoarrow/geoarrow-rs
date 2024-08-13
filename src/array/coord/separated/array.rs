use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
// use arrow2::array::{Array, PrimitiveArray, StructArray};
use arrow_array::{Array, ArrayRef, Float64Array, StructArray};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

use crate::array::{CoordType, SeparatedCoordBufferBuilder};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::CoordTrait;
use crate::scalar::SeparatedCoord;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::GeometryArrayTrait;

#[derive(Debug, Clone, PartialEq)]
pub struct SeparatedCoordBuffer<const D: usize> {
    pub(crate) buffers: [ScalarBuffer<f64>; D],
}

fn check<const D: usize>(buffers: &[ScalarBuffer<f64>; D]) -> Result<()> {
    if !buffers.windows(2).all(|w| w[0].len() == w[1].len()) {
        return Err(GeoArrowError::General(
            "all buffers must have the same length".to_string(),
        ));
    }

    Ok(())
}

impl<const D: usize> SeparatedCoordBuffer<D> {
    /// Construct a new SeparatedCoordBuffer
    ///
    /// # Panics
    ///
    /// - if the x and y buffers have different lengths
    pub fn new(buffers: [ScalarBuffer<f64>; D]) -> Self {
        check(&buffers).unwrap();
        Self { buffers }
    }

    /// Construct a new SeparatedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the x and y buffers have different lengths
    pub fn try_new(buffers: [ScalarBuffer<f64>; D]) -> Result<Self> {
        check(&buffers)?;
        Ok(Self { buffers })
    }

    pub fn values_array(&self) -> Vec<ArrayRef> {
        self.buffers
            .iter()
            .map(|buffer| Arc::new(Float64Array::new(buffer.clone(), None)) as ArrayRef)
            .collect()
    }

    pub fn values_field(&self) -> Vec<Field> {
        match D {
            2 => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]
            }
            3 => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("z", DataType::Float64, false),
                ]
            }
            _ => todo!("only supports xy and xyz right now."),
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
}

impl<const D: usize> GeometryArrayTrait for SeparatedCoordBuffer<D> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> crate::datatypes::GeoDataType {
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

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Separated
    }

    fn to_coord_type(&self, _coord_type: CoordType) -> Arc<dyn GeometryArrayTrait> {
        panic!()
    }

    fn metadata(&self) -> Arc<crate::array::metadata::ArrayMetadata> {
        panic!()
    }

    fn with_metadata(
        &self,
        _metadata: Arc<crate::array::metadata::ArrayMetadata>,
    ) -> crate::trait_::GeometryArrayRef {
        panic!()
    }

    fn len(&self) -> usize {
        self.buffers[0].len()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        panic!("coordinate arrays don't have their own validity arrays")
    }

    fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self
    }
}

impl<const D: usize> GeometryArraySelfMethods<D> for SeparatedCoordBuffer<D> {
    fn with_coords(self, _coords: crate::array::CoordBuffer<D>) -> Self {
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

        // Initialize array with existing buffers, then overwrite them
        let mut sliced_buffers = self.buffers.clone();
        for (i, buffer) in self.buffers.iter().enumerate() {
            sliced_buffers[i] = buffer.slice(offset, length);
        }

        Self {
            buffers: sliced_buffers,
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );

        // Initialize array with existing buffers, then overwrite them
        let mut sliced_buffers = self.buffers.clone();
        for (i, buffer) in self.buffers.iter().enumerate() {
            sliced_buffers[i] = buffer.slice(offset, length).to_vec().into();
        }

        Self {
            buffers: sliced_buffers,
        }
    }
}

impl<'a, const D: usize> GeometryArrayAccessor<'a> for SeparatedCoordBuffer<D> {
    type Item = SeparatedCoord<'a, D>;
    type ItemGeo = geo::Coord;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        SeparatedCoord {
            buffers: &self.buffers,
            i: index,
        }
    }
}

impl<const D: usize> IntoArrow for SeparatedCoordBuffer<D> {
    type ArrowArray = StructArray;

    fn into_arrow(self) -> Self::ArrowArray {
        StructArray::new(self.values_field().into(), self.values_array(), None)
    }
}

impl<const D: usize> From<SeparatedCoordBuffer<D>> for StructArray {
    fn from(value: SeparatedCoordBuffer<D>) -> Self {
        value.into_arrow()
    }
}

impl<const D: usize> TryFrom<&StructArray> for SeparatedCoordBuffer<D> {
    type Error = GeoArrowError;

    fn try_from(value: &StructArray) -> Result<Self> {
        let arrays = value.columns();

        if !arrays.len() == 2 {
            return Err(GeoArrowError::General(
                "Expected {D} child arrays of this StructArray.".to_string(),
            ));
        }

        let buffers =
            core::array::from_fn(|i| arrays[i].as_primitive::<Float64Type>().values().clone());
        Ok(Self::new(buffers))
        // let buffers = [ScalarBuffer::<f64>::from(vec![]); D];

        // let x_array_values = arrays[0].as_any().downcast_ref::<Float64Array>().unwrap();
        // let y_array_values = arrays[1].as_any().downcast_ref::<Float64Array>().unwrap();

        // Ok(SeparatedCoordBuffer::new(
        //     x_array_values.values().clone(),
        //     y_array_values.values().clone(),
        // ))
    }
}

impl TryFrom<(Vec<f64>, Vec<f64>)> for SeparatedCoordBuffer<2> {
    type Error = GeoArrowError;

    fn try_from(value: (Vec<f64>, Vec<f64>)) -> std::result::Result<Self, Self::Error> {
        Self::try_new([value.0.into(), value.1.into()])
    }
}

impl<G: CoordTrait<T = f64>> From<&[G]> for SeparatedCoordBuffer<2> {
    fn from(other: &[G]) -> Self {
        let mut_arr: SeparatedCoordBufferBuilder<2> = other.into();
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

        let buf1 = SeparatedCoordBuffer::new([x1.into(), y1.into()]).slice(1, 1);
        dbg!(&buf1.buffers[0]);
        dbg!(&buf1.buffers[1]);

        let x2 = vec![1.];
        let y2 = vec![4.];
        let buf2 = SeparatedCoordBuffer::new([x2.into(), y2.into()]);

        assert_eq!(buf1, buf2);
    }
}
