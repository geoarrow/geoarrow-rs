use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array, StructArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field};

use crate::array::{CoordType, SeparatedCoordBufferBuilder};
use crate::datatypes::{coord_type_to_data_type, Dimension};
use crate::error::{GeoArrowError, Result};
use crate::scalar::SeparatedCoord;
use crate::trait_::IntoArrow;
use geo_traits::CoordTrait;

#[derive(Debug, Clone, PartialEq)]
pub struct SeparatedCoordBuffer {
    /// We always store a buffer for all 4 dimensions. The buffers for dimension 3 and 4 may be
    /// empty.
    pub(crate) buffers: [ScalarBuffer<f64>; 4],
    pub(crate) dim: Dimension,
}

fn check(buffers: &[ScalarBuffer<f64>; 4], _dim: Dimension) -> Result<()> {
    if !buffers.windows(2).all(|w| w[0].len() == w[1].len()) {
        return Err(GeoArrowError::General(
            "all buffers must have the same length".to_string(),
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
    pub fn new(buffers: [ScalarBuffer<f64>; 4], dim: Dimension) -> Self {
        Self::try_new(buffers, dim).unwrap()
    }

    /// Construct a new SeparatedCoordBuffer
    ///
    /// # Errors
    ///
    /// - if the x and y buffers have different lengths
    pub fn try_new(buffers: [ScalarBuffer<f64>; 4], dim: Dimension) -> Result<Self> {
        check(&buffers, dim)?;
        Ok(Self { buffers, dim })
    }

    /// Access the underlying coordinate buffers.
    pub(crate) fn coords(&self) -> &[ScalarBuffer<f64>; 4] {
        &self.buffers
    }

    pub fn dim(&self) -> Dimension {
        self.dim
    }

    pub fn values_array(&self) -> Vec<ArrayRef> {
        self.buffers
            .iter()
            .map(|buffer| Arc::new(Float64Array::new(buffer.clone(), None)) as ArrayRef)
            .collect()
    }

    pub fn values_field(&self) -> Vec<Field> {
        match self.dim {
            Dimension::XY => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                ]
            }
            Dimension::XYZ => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("z", DataType::Float64, false),
                ]
            }
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
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
            dim: self.dim,
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
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
            dim: self.dim,
        }
    }

    pub fn storage_type(&self) -> DataType {
        coord_type_to_data_type(CoordType::Separated, self.dim)
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    pub fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    pub fn coord_type(&self) -> CoordType {
        CoordType::Separated
    }

    pub fn len(&self) -> usize {
        self.buffers[0].len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, index: usize) -> SeparatedCoord<'_> {
        assert!(index <= self.len());
        self.value_unchecked(index)
    }

    pub fn value_unchecked(&self, index: usize) -> SeparatedCoord<'_> {
        SeparatedCoord {
            buffers: &self.buffers,
            i: index,
            dim: self.dim,
        }
    }

    pub fn from_arrow(array: &StructArray, dim: Dimension) -> Result<Self> {
        let arrays = array.columns();
        assert_eq!(arrays.len(), dim.size());

        // Initialize buffers with empty array, then mutate into it
        let mut buffers = core::array::from_fn(|_| vec![].into());
        for i in 0..arrays.len() {
            buffers[i] = arrays[i].as_primitive::<Float64Type>().values().clone();
        }

        Self::try_new(buffers, dim)
    }

    pub fn from_coords<G: CoordTrait<T = f64>>(coords: &[G], dim: Dimension) -> Result<Self> {
        Ok(SeparatedCoordBufferBuilder::from_coords(coords, dim)?.into())
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

impl TryFrom<(Vec<f64>, Vec<f64>)> for SeparatedCoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: (Vec<f64>, Vec<f64>)) -> std::result::Result<Self, Self::Error> {
        Self::try_new(
            [value.0.into(), value.1.into(), vec![].into(), vec![].into()],
            Dimension::XY,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_eq_slicing() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let buf1 = SeparatedCoordBuffer::new(
            [x1.into(), y1.into(), vec![].into(), vec![].into()],
            Dimension::XY,
        )
        .slice(1, 1);
        dbg!(&buf1.buffers[0]);
        dbg!(&buf1.buffers[1]);

        let x2 = vec![1.];
        let y2 = vec![4.];
        let buf2 = SeparatedCoordBuffer::new(
            [x2.into(), y2.into(), vec![].into(), vec![].into()],
            Dimension::XY,
        );

        assert_eq!(buf1, buf2);
    }
}
