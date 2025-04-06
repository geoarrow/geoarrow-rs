use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_array::{ArrayRef, Float64Array, StructArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{CoordType, Dimension, PointType};

use crate::array::SeparatedCoordBufferBuilder;
use crate::error::{GeoArrowError, Result};
use crate::scalar::SeparatedCoord;
use crate::trait_::IntoArrow;
use geo_traits::CoordTrait;

/// The GeoArrow equivalent to `Vec<Option<Coord>>`: an immutable collection of coordinates.
///
/// This stores all coordinates in separated fashion as multiple underlying buffers: `xxx` and
/// `yyy`.
#[derive(Debug, Clone, PartialEq)]
pub struct SeparatedCoordBuffer {
    /// We always store a buffer for all 4 dimensions. The buffers for dimension 3 and 4 may be
    /// empty.
    pub(crate) buffers: [ScalarBuffer<f64>; 4],
    pub(crate) dim: Dimension,
}

fn check(buffers: &[ScalarBuffer<f64>; 4], dim: Dimension) -> Result<()> {
    let all_same_length = match dim {
        Dimension::XY => buffers[0].len() == buffers[1].len(),
        Dimension::XYZ | Dimension::XYM => {
            buffers[0].len() == buffers[1].len() && buffers[1].len() == buffers[2].len()
        }
        Dimension::XYZM => {
            buffers[0].len() == buffers[1].len()
                && buffers[1].len() == buffers[2].len()
                && buffers[2].len() == buffers[3].len()
        }
    };

    if !all_same_length {
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
    ///
    /// Note that not all four buffers may be valid. Only so many buffers have defined meaning as
    /// there are dimensions, so for an XY buffer, only the first two buffers have defined meaning,
    /// and the last two may be any buffer, or empty.
    pub fn raw_buffers(&self) -> &[ScalarBuffer<f64>; 4] {
        &self.buffers
    }

    /// Access the underlying coordinate buffers.
    ///
    /// In comparison to raw_buffers, all of the returned buffers are valid.
    pub fn buffers(&self) -> Vec<ScalarBuffer<f64>> {
        match self.dim {
            Dimension::XY => {
                vec![self.buffers[0].clone(), self.buffers[1].clone()]
            }
            Dimension::XYZ | Dimension::XYM => {
                vec![
                    self.buffers[0].clone(),
                    self.buffers[1].clone(),
                    self.buffers[2].clone(),
                ]
            }
            Dimension::XYZM => {
                vec![
                    self.buffers[0].clone(),
                    self.buffers[1].clone(),
                    self.buffers[2].clone(),
                    self.buffers[3].clone(),
                ]
            }
        }
    }

    /// The dimension of this coordinate buffer
    pub fn dim(&self) -> Dimension {
        self.dim
    }

    pub(crate) fn values_array(&self) -> Vec<ArrayRef> {
        match self.dim {
            Dimension::XY => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                ]
            }
            Dimension::XYZ | Dimension::XYM => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[2].clone(), None)),
                ]
            }
            Dimension::XYZM => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[2].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[3].clone(), None)),
                ]
            }
        }
    }

    pub(crate) fn values_field(&self) -> Vec<Field> {
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
            Dimension::XYM => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("m", DataType::Float64, false),
                ]
            }
            Dimension::XYZM => {
                vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float64, false),
                    Field::new("z", DataType::Float64, false),
                    Field::new("m", DataType::Float64, false),
                ]
            }
        }
    }

    pub(crate) fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );

        // Initialize array with existing buffers, then overwrite them
        let mut sliced_buffers = self.buffers.clone();
        for (i, buffer) in self.buffers.iter().enumerate().take(self.dim.size()) {
            sliced_buffers[i] = buffer.slice(offset, length);
        }

        Self {
            buffers: sliced_buffers,
            dim: self.dim,
        }
    }

    pub(crate) fn storage_type(&self) -> DataType {
        PointType::new(CoordType::Separated, self.dim).data_type()
    }

    /// The coordinate type
    pub fn coord_type(&self) -> CoordType {
        CoordType::Separated
    }

    /// The number of coordinates
    pub fn len(&self) -> usize {
        self.buffers[0].len()
    }

    /// Whether the coordinate buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn value(&self, index: usize) -> SeparatedCoord<'_> {
        assert!(index <= self.len());
        self.value_unchecked(index)
    }

    pub(crate) fn value_unchecked(&self, index: usize) -> SeparatedCoord<'_> {
        SeparatedCoord {
            buffers: &self.buffers,
            i: index,
            dim: self.dim,
        }
    }

    pub(crate) fn from_arrow(array: &StructArray, dim: Dimension) -> Result<Self> {
        let arrays = array.columns();
        assert_eq!(arrays.len(), dim.size());

        // Initialize buffers with empty array, then mutate into it
        let mut buffers = core::array::from_fn(|_| vec![].into());
        for i in 0..arrays.len() {
            buffers[i] = arrays[i].as_primitive::<Float64Type>().values().clone();
        }

        Self::try_new(buffers, dim)
    }

    /// Construct from an iterator of coordinates
    pub fn from_coords<'a>(
        coords: impl ExactSizeIterator<Item = &'a (impl CoordTrait<T = f64> + 'a)>,
        dim: Dimension,
    ) -> Result<Self> {
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
