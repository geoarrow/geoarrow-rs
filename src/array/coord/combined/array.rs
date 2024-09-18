use std::sync::Arc;

use crate::array::{
    CoordType, InterleavedCoordBuffer, InterleavedCoordBufferBuilder, SeparatedCoordBuffer,
    SeparatedCoordBufferBuilder,
};
use crate::error::GeoArrowError;
use crate::scalar::Coord;
use crate::trait_::IntoArrow;
use arrow_array::{Array, FixedSizeListArray, StructArray};
use arrow_schema::DataType;

/// An Arrow representation of an array of coordinates.
///
/// As defined in the GeoArrow spec, coordinates can either be interleaved (i.e. a single array of
/// XYXYXY) or separated (i.e. two arrays, one XXX and another YYY).
///
/// This CoordBuffer abstracts over an `InterleavedCoordBuffer` and a `SeparatedCoordBuffer`.
///
/// For now all coordinate buffers support only two dimensions.
///
/// This is named `CoordBuffer` instead of `CoordArray` because the buffer does not store its own
/// validity bitmask. Rather the geometry arrays that build on top of this maintain their own
/// validity masks.
#[derive(Debug, Clone)]
pub enum CoordBuffer<const D: usize> {
    Interleaved(InterleavedCoordBuffer<D>),
    Separated(SeparatedCoordBuffer<D>),
}

impl<const D: usize> CoordBuffer<D> {
    pub fn get_x(&self, i: usize) -> f64 {
        match self {
            CoordBuffer::Interleaved(c) => c.get_x(i),
            CoordBuffer::Separated(c) => c.get_x(i),
        }
    }

    pub fn get_y(&self, i: usize) -> f64 {
        match self {
            CoordBuffer::Interleaved(c) => c.get_y(i),
            CoordBuffer::Separated(c) => c.get_y(i),
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        match self {
            CoordBuffer::Interleaved(c) => CoordBuffer::Interleaved(c.slice(offset, length)),
            CoordBuffer::Separated(c) => CoordBuffer::Separated(c.slice(offset, length)),
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
        match self {
            CoordBuffer::Interleaved(cb) => {
                CoordBuffer::Interleaved(cb.owned_slice(offset, length))
            }
            CoordBuffer::Separated(cb) => CoordBuffer::Separated(cb.owned_slice(offset, length)),
        }
    }

    pub fn coord_type(&self) -> CoordType {
        match self {
            CoordBuffer::Interleaved(cb) => cb.coord_type(),
            CoordBuffer::Separated(cb) => cb.coord_type(),
        }
    }

    pub fn storage_type(&self) -> DataType {
        match self {
            CoordBuffer::Interleaved(c) => c.storage_type(),
            CoordBuffer::Separated(c) => c.storage_type(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CoordBuffer::Interleaved(c) => c.len(),
            CoordBuffer::Separated(c) => c.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, index: usize) -> Coord<'_, D> {
        match self {
            CoordBuffer::Interleaved(c) => Coord::Interleaved(c.value(index)),
            CoordBuffer::Separated(c) => Coord::Separated(c.value(index)),
        }
    }

    pub fn into_array_ref(self) -> Arc<dyn Array> {
        self.into_arrow()
    }

    pub fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    pub fn with_coords(self, coords: CoordBuffer<D>) -> Self {
        assert_eq!(coords.len(), self.len());
        coords
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        match (self, coord_type) {
            (CoordBuffer::Interleaved(cb), CoordType::Interleaved) => CoordBuffer::Interleaved(cb),
            (CoordBuffer::Interleaved(cb), CoordType::Separated) => {
                let mut new_buffer = SeparatedCoordBufferBuilder::with_capacity(cb.len());
                let coords = cb.coords;
                for row_start_idx in (0..coords.len()).step_by(D) {
                    new_buffer.push(core::array::from_fn(|i| coords[row_start_idx + i]));
                }
                CoordBuffer::Separated(new_buffer.into())
            }
            (CoordBuffer::Separated(cb), CoordType::Separated) => CoordBuffer::Separated(cb),
            (CoordBuffer::Separated(cb), CoordType::Interleaved) => {
                let mut new_buffer = InterleavedCoordBufferBuilder::with_capacity(cb.len());
                for row_idx in 0..cb.len() {
                    new_buffer.push(core::array::from_fn(|i| cb.buffers[i][row_idx]));
                }
                CoordBuffer::Interleaved(new_buffer.into())
            }
        }
    }
}

impl<const D: usize> IntoArrow for CoordBuffer<D> {
    type ArrowArray = Arc<dyn Array>;

    fn into_arrow(self) -> Self::ArrowArray {
        match self {
            CoordBuffer::Interleaved(c) => Arc::new(c.into_arrow()),
            CoordBuffer::Separated(c) => Arc::new(c.into_arrow()),
        }
    }
}

impl<const D: usize> TryFrom<&dyn Array> for CoordBuffer<D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Struct(_) => {
                let downcasted = value.as_any().downcast_ref::<StructArray>().unwrap();
                Ok(CoordBuffer::Separated(downcasted.try_into()?))
            }
            DataType::FixedSizeList(_, _) => {
                let downcasted = value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                Ok(CoordBuffer::Interleaved(downcasted.try_into()?))
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<const D: usize> PartialEq for CoordBuffer<D> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CoordBuffer::Interleaved(a), CoordBuffer::Interleaved(b)) => PartialEq::eq(a, b),
            (CoordBuffer::Interleaved(left), CoordBuffer::Separated(right)) => {
                if left.len() != right.len() {
                    return false;
                }

                for i in 0..left.len() {
                    let left_coord = left.value(i);
                    let right_coord = right.value(i);

                    if left_coord != right_coord {
                        return false;
                    }
                }

                true
            }
            (CoordBuffer::Separated(a), CoordBuffer::Separated(b)) => PartialEq::eq(a, b),
            (CoordBuffer::Separated(left), CoordBuffer::Interleaved(right)) => {
                if left.len() != right.len() {
                    return false;
                }

                for i in 0..left.len() {
                    let left_coord = left.value(i);
                    let right_coord = right.value(i);

                    if left_coord != right_coord {
                        return false;
                    }
                }

                true
            }
        }
    }
}

impl<const D: usize> From<InterleavedCoordBuffer<D>> for CoordBuffer<D> {
    fn from(value: InterleavedCoordBuffer<D>) -> Self {
        Self::Interleaved(value)
    }
}

impl<const D: usize> From<SeparatedCoordBuffer<D>> for CoordBuffer<D> {
    fn from(value: SeparatedCoordBuffer<D>) -> Self {
        Self::Separated(value)
    }
}

#[cfg(test)]
mod test {
    use crate::error::Result;

    use super::*;

    #[test]
    fn test_eq_both_interleaved() -> Result<()> {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = CoordBuffer::<2>::Interleaved(coords1.try_into()?);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = CoordBuffer::Interleaved(coords2.try_into()?);

        assert_eq!(buf1, buf2);
        Ok(())
    }

    #[test]
    fn test_eq_across_types() -> Result<()> {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let buf1 = CoordBuffer::Separated((x1, y1).try_into()?);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = CoordBuffer::Interleaved(coords2.try_into()?);

        assert_eq!(buf1, buf2);
        Ok(())
    }

    #[test]
    fn test_eq_across_types_slicing() -> Result<()> {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let buf1 = CoordBuffer::Separated((x1, y1).try_into()?).slice(1, 1);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = CoordBuffer::Interleaved(coords2.try_into()?).slice(1, 1);

        assert_eq!(buf1, buf2);
        Ok(())
    }
}
