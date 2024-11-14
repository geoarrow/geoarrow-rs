use std::sync::Arc;

use crate::array::{
    CoordType, InterleavedCoordBuffer, InterleavedCoordBufferBuilder, SeparatedCoordBuffer,
    SeparatedCoordBufferBuilder,
};
use crate::datatypes::Dimension;
use crate::error::{GeoArrowError, Result};
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
pub enum CoordBuffer {
    Interleaved(InterleavedCoordBuffer),
    Separated(SeparatedCoordBuffer),
}

impl CoordBuffer {
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

    pub fn value(&self, index: usize) -> Coord<'_> {
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

    pub fn dim(&self) -> Dimension {
        match self {
            CoordBuffer::Interleaved(c) => c.dim(),
            CoordBuffer::Separated(c) => c.dim(),
        }
    }

    pub fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.len());
        coords
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        let dim = self.dim();
        match (self, coord_type) {
            (CoordBuffer::Interleaved(cb), CoordType::Interleaved) => CoordBuffer::Interleaved(cb),
            (CoordBuffer::Interleaved(cb), CoordType::Separated) => {
                let mut new_buffer = SeparatedCoordBufferBuilder::with_capacity(cb.len(), dim);
                for i in 0..cb.len() {
                    let coord = cb.value(i);
                    new_buffer.push_coord(&coord);
                }
                CoordBuffer::Separated(new_buffer.into())
            }
            (CoordBuffer::Separated(cb), CoordType::Separated) => CoordBuffer::Separated(cb),
            (CoordBuffer::Separated(cb), CoordType::Interleaved) => {
                let mut new_buffer = InterleavedCoordBufferBuilder::with_capacity(cb.len(), dim);
                for i in 0..cb.len() {
                    let coord = cb.value(i);
                    new_buffer.push_coord(&coord);
                }
                CoordBuffer::Interleaved(new_buffer.into())
            }
        }
    }

    pub fn from_arrow(value: &dyn Array, dim: Dimension) -> Result<Self> {
        match value.data_type() {
            DataType::Struct(_) => {
                let downcasted = value.as_any().downcast_ref::<StructArray>().unwrap();
                Ok(CoordBuffer::Separated(SeparatedCoordBuffer::from_arrow(
                    downcasted, dim,
                )?))
            }
            DataType::FixedSizeList(_, _) => {
                let downcasted = value.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                Ok(CoordBuffer::Interleaved(
                    InterleavedCoordBuffer::from_arrow(downcasted, dim)?,
                ))
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl IntoArrow for CoordBuffer {
    type ArrowArray = Arc<dyn Array>;

    fn into_arrow(self) -> Self::ArrowArray {
        match self {
            CoordBuffer::Interleaved(c) => Arc::new(c.into_arrow()),
            CoordBuffer::Separated(c) => Arc::new(c.into_arrow()),
        }
    }
}

impl PartialEq for CoordBuffer {
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

impl From<InterleavedCoordBuffer> for CoordBuffer {
    fn from(value: InterleavedCoordBuffer) -> Self {
        Self::Interleaved(value)
    }
}

impl From<SeparatedCoordBuffer> for CoordBuffer {
    fn from(value: SeparatedCoordBuffer) -> Self {
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
        let buf1 =
            CoordBuffer::Interleaved(InterleavedCoordBuffer::from_vec(coords1, Dimension::XY)?);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 =
            CoordBuffer::Interleaved(InterleavedCoordBuffer::from_vec(coords2, Dimension::XY)?);

        assert_eq!(buf1, buf2);
        Ok(())
    }

    #[test]
    fn test_eq_across_types() -> Result<()> {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let buf1 = CoordBuffer::Separated(SeparatedCoordBuffer::new(
            [x1.into(), y1.into(), vec![].into(), vec![].into()],
            Dimension::XY,
        ));

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 =
            CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords2.into(), Dimension::XY));

        assert_eq!(buf1, buf2);
        Ok(())
    }

    #[test]
    fn test_eq_across_types_slicing() -> Result<()> {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];

        let buf1 = CoordBuffer::Separated((x1, y1).try_into()?).slice(1, 1);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 =
            CoordBuffer::Interleaved(InterleavedCoordBuffer::new(coords2.into(), Dimension::XY))
                .slice(1, 1);

        assert_eq!(buf1, buf2);
        Ok(())
    }
}
