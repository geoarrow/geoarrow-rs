use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float64Array, StructArray};
use arrow_buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

use crate::algorithm::native::eq::coord_eq;
use crate::array::{CoordType, InterleavedCoordBufferBuilder, SeparatedCoordBufferBuilder};
use crate::datatypes::{coord_type_to_data_type, Dimension};
use crate::error::{GeoArrowError, Result};
use crate::scalar::Coord;
use crate::trait_::IntoArrow;

/// A GeoArrow coordinate buffer
#[derive(Clone, Debug)]
pub struct CoordBuffer {
    /// We always store 4 buffers in an array, but not all 4 of these may have valid coordinates.
    /// The number of valid buffers is stored in `num_buffers` and matches the physical size of the
    /// dimension. For example, `XY` coordinates will only have two valid buffers, in slots `0` and
    /// `1`. `XYZ` and `XYM` will have three valid buffers and `XYZM` will have four valid buffers.
    ///
    /// In the case of interleaved coordinates, each slot will be a clone of the same
    /// reference-counted buffer.
    pub(crate) buffers: [ScalarBuffer<f64>; 4],

    /// The number of coordinates in this buffer
    pub(crate) num_coords: usize,

    /// The number of valid buffers in the buffers array. (i.e., number of physical dimensions)
    /// TODO: unsure if this is needed since we also store the logical dimension? Maybe still
    /// faster than doing the Dimension enum lookup on coord access.
    pub(crate) num_buffers: usize,

    /// The number of elements to advance a given value pointer to the next ordinate.
    ///
    /// - For interleaved coordinates, `coords_stride` will equal `num_buffers`.
    /// - For struct coordinates, `coords_stride` will be 1.
    pub(crate) coords_stride: usize,

    /// The coordinate type of this buffer (interleaved or separated).
    pub(crate) coord_type: CoordType,

    /// The dimension of this buffer (e.g. `XY`, `XYZ`).
    pub(crate) dim: Dimension,
}

impl CoordBuffer {
    /// Construct a new buffer from interleaved coordinates.
    pub fn new_interleaved(coords: ScalarBuffer<f64>, dim: Dimension) -> Result<Self> {
        if coords.len() % dim.size() != 0 {
            return Err(GeoArrowError::General(format!(
                "Coordinate length {} should be a multiple of physical dimension {:?}",
                coords.len(),
                dim
            )));
        }

        // These buffers are reference-counted and clones are cheap
        let buffers = [
            coords.clone(),
            coords.clone(),
            coords.clone(),
            coords.clone(),
        ];

        Ok(Self {
            buffers,
            num_coords: coords.len() / dim.size(),
            num_buffers: 1,
            coords_stride: dim.size(),
            coord_type: CoordType::Interleaved,
            dim,
        })
    }

    /// Construct a new buffer from separated coordinates.
    ///
    /// - All coordinate buffers must have the same length.
    ///
    /// This takes a slice of coords because cloning is cheap and that allows both vec and array
    /// input.
    pub fn new_separated(coords: &[ScalarBuffer<f64>], dim: Dimension) -> Result<Self> {
        if !coords.windows(2).all(|w| w[0].len() == w[1].len()) {
            return Err(GeoArrowError::General(
                "all input coordinate buffers must have the same length".to_string(),
            ));
        }

        let num_buffers = match dim {
            Dimension::XY => 2,
            Dimension::XYZ => 3,
        };
        if coords.len() != num_buffers {
            return Err(GeoArrowError::General(format!(
                "Expected {} buffers, got {}",
                num_buffers,
                coords.len()
            )));
        }

        let empty_buffer = ScalarBuffer::from(Buffer::from_vec(Vec::<f64>::new()));
        let buffers = match num_buffers {
            2 => [
                coords[0].clone(),
                coords[1].clone(),
                empty_buffer.clone(),
                empty_buffer.clone(),
            ],
            3 => [
                coords[0].clone(),
                coords[1].clone(),
                coords[2].clone(),
                empty_buffer.clone(),
            ],
            4 => [
                coords[0].clone(),
                coords[1].clone(),
                coords[2].clone(),
                coords[3].clone(),
            ],
            _ => unreachable!(),
        };

        Ok(Self {
            buffers,
            num_coords: coords[0].len(),
            num_buffers,
            coords_stride: 1,
            coord_type: CoordType::Separated,
            dim,
        })
    }

    pub fn from_arrow(array: &dyn Array, dim: Dimension) -> Result<Self> {
        match array.data_type() {
            DataType::FixedSizeList(inner_field, inner_size) => {
                todo!()
            }
            DataType::Struct(inner_fields) => {
                todo!()
            }
            dt => Err(GeoArrowError::General(format!(
                "Unexpected data type in from_arrow: {}",
                dt
            ))),
        }
    }

    pub fn coord_type(&self) -> CoordType {
        self.coord_type
    }

    pub fn len(&self) -> usize {
        self.num_coords
    }

    /// Convert this coordinate buffer to another coord type.
    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        match (self.coord_type, coord_type) {
            (CoordType::Interleaved, CoordType::Interleaved) => self,
            (CoordType::Interleaved, CoordType::Separated) => {
                // let mut new_buffer = SeparatedCoordBufferBuilder::with_capacity(self.num_coords);
                // for i in 0..self.num_coords {
                //     new_buffer.push_coord(&self.value_unchecked(i));
                // }

                todo!()
                // Self::new_separated(new_buffer.buffers, dim)
            }
            (CoordType::Separated, CoordType::Separated) => self,
            (CoordType::Separated, CoordType::Interleaved) => {
                todo!()
                // let mut new_buffer = InterleavedCoordBufferBuilder::with_capacity(self.num_coords);
                // for row_idx in 0..cb.len() {
                //     new_buffer.push(core::array::from_fn(|i| cb.buffers[i][row_idx]));
                // }
                // CoordBuffer::Interleaved(new_buffer.into())
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        todo!()
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
        todo!()
    }

    pub fn value(&self, index: usize) -> Coord<'_> {
        assert!(index <= self.len());
        self.value_unchecked(index)
    }

    pub fn value_unchecked(&self, index: usize) -> Coord<'_> {
        Coord {
            buffer: self,
            i: index,
        }
    }

    fn interleaved_values_array(&self) -> Float64Array {
        debug_assert_eq!(self.coord_type, CoordType::Interleaved);
        Float64Array::new(self.buffers[0].clone(), None)
    }

    fn interleaved_values_field(&self) -> Field {
        debug_assert_eq!(self.coord_type, CoordType::Interleaved);
        match self.dim {
            Dimension::XY => Field::new("xy", DataType::Float64, false),
            Dimension::XYZ => Field::new("xyz", DataType::Float64, false),
        }
    }

    fn separated_values_array(&self) -> Vec<ArrayRef> {
        debug_assert_eq!(self.coord_type, CoordType::Separated);
        match self.dim {
            Dimension::XY => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                ]
            }
            Dimension::XYZ => {
                vec![
                    Arc::new(Float64Array::new(self.buffers[0].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[1].clone(), None)),
                    Arc::new(Float64Array::new(self.buffers[2].clone(), None)),
                ]
            }
        }
    }

    fn separated_values_field(&self) -> Vec<Field> {
        debug_assert_eq!(self.coord_type, CoordType::Separated);
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

    pub fn storage_type(&self) -> DataType {
        coord_type_to_data_type(self.coord_type, self.dim)
    }

    pub(crate) fn into_arrow_with_validity(self, nulls: Option<NullBuffer>) -> ArrayRef {
        match self.coord_type {
            CoordType::Separated => Arc::new(StructArray::new(
                self.separated_values_field().into(),
                self.separated_values_array(),
                nulls,
            )),
            CoordType::Interleaved => Arc::new(FixedSizeListArray::new(
                Arc::new(self.interleaved_values_field()),
                self.dim.size().try_into().unwrap(),
                Arc::new(self.interleaved_values_array()),
                nulls,
            )),
        }
    }
}

impl IntoArrow for CoordBuffer {
    type ArrowArray = ArrayRef;

    fn into_arrow(self) -> Self::ArrowArray {
        self.into_arrow_with_validity(None)
    }
}

impl PartialEq for CoordBuffer {
    fn eq(&self, other: &Self) -> bool {
        if self.num_coords != other.num_coords {
            return false;
        }

        for i in 0..self.num_coords {
            let left = self.value_unchecked(i);
            let right = other.value_unchecked(i);

            if !coord_eq(&left, &right) {
                return false;
            }
        }

        true
    }
}

impl TryFrom<&FixedSizeListArray> for CoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &FixedSizeListArray) -> std::result::Result<Self, Self::Error> {
        let dim = Dimension::try_from(value.value_length() as usize).unwrap();
        let coord_array_values = value
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        CoordBuffer::new_interleaved(coord_array_values.values().clone(), dim)
    }
}

impl TryFrom<&StructArray> for CoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &StructArray) -> std::result::Result<Self, Self::Error> {
        let arrays = value.columns();
        let dim = Dimension::try_from(arrays.len()).unwrap();
        let buffers = arrays
            .iter()
            .map(|arr| arr.as_primitive::<Float64Type>().values().clone())
            .collect::<Vec<_>>();

        CoordBuffer::new_separated(&buffers, dim)
    }
}

impl TryFrom<&dyn Array> for CoordBuffer {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Struct(_) => {
                let downcasted = value.as_any().downcast_ref::<StructArray>().unwrap();
                downcasted.try_into()
            }
            DataType::FixedSizeList(_, _) => {
                let downcasted = value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}
