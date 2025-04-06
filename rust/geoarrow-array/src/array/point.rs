use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, FixedSizeListArray, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, PointType};

use crate::array::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
use crate::datatypes::NativeType;
use crate::eq::point_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::Point;
use crate::trait_::{ArrayAccessor, ArrayBase, IntoArrow, NativeArray};

/// An immutable array of Point geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<Point>>` due to the internal validity bitmap.
#[derive(Debug, Clone)]
pub struct PointArray {
    pub(crate) data_type: PointType,
    pub(crate) coords: CoordBuffer,
    pub(crate) validity: Option<NullBuffer>,
}

/// Perform checks:
///
/// - Validity mask must have the same length as the coordinates.
pub(super) fn check(coords: &CoordBuffer, validity_len: Option<usize>) -> Result<()> {
    if validity_len.is_some_and(|len| len != coords.len()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    Ok(())
}

impl PointArray {
    /// Create a new PointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    pub fn new(coords: CoordBuffer, validity: Option<NullBuffer>, metadata: Arc<Metadata>) -> Self {
        Self::try_new(coords, validity, metadata).unwrap()
    }

    /// Create a new PointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    pub fn try_new(
        coords: CoordBuffer,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(&coords, validity.as_ref().map(|v| v.len()))?;
        Ok(Self {
            data_type: PointType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            validity,
        })
    }

    /// Access the underlying coordinate buffer
    ///
    /// Note that some coordinates may be null, depending on the value of [`Self::nulls`]
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    /// Access the
    pub fn into_inner(self) -> (CoordBuffer, Option<NullBuffer>) {
        (self.coords, self.validity)
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> usize {
        self.len()
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let dimension = self.data_type.dimension();
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths() * dimension.size() * 8
    }

    /// Slices this [`PointArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            data_type: self.data_type.clone(),
            coords: self.coords.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }
}

impl ArrayBase for PointArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        self.into_arrow()
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.coords.len()
    }

    /// Returns the optional validity.
    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl NativeArray for PointArray {
    fn data_type(&self) -> NativeType {
        NativeType::Point(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for PointArray {
    type Item = Point<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Point::new(&self.coords, index)
    }
}

impl IntoArrow for PointArray {
    type ArrowArray = ArrayRef;
    type ExtensionType = PointType;

    fn into_arrow(self) -> Self::ArrowArray {
        let validity = self.validity;
        let dim = self.coords.dim();
        match self.coords {
            CoordBuffer::Interleaved(c) => Arc::new(FixedSizeListArray::new(
                c.values_field().into(),
                dim.size() as i32,
                Arc::new(c.values_array()),
                validity,
            )),
            CoordBuffer::Separated(c) => {
                let fields = c.values_field();
                Arc::new(StructArray::new(fields.into(), c.values_array(), validity))
            }
        }
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&FixedSizeListArray, PointType)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&FixedSizeListArray, PointType)) -> Result<Self> {
        let interleaved_coords = InterleavedCoordBuffer::from_arrow(value, typ.dimension())?;

        Ok(Self::new(
            CoordBuffer::Interleaved(interleaved_coords),
            value.nulls().cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&StructArray, PointType)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&StructArray, PointType)) -> Result<Self> {
        let validity = value.nulls();
        let separated_coords = SeparatedCoordBuffer::from_arrow(value, typ.dimension())?;
        Ok(Self::new(
            CoordBuffer::Separated(separated_coords),
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, PointType)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, PointType)) -> Result<Self> {
        match value.data_type() {
            DataType::FixedSizeList(_, _) => (value.as_fixed_size_list(), typ).try_into(),
            DataType::Struct(_) => (value.as_struct(), typ).try_into(),
            _ => Err(GeoArrowError::General(
                "Invalid data type for PointArray".to_string(),
            )),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for PointArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<PointType>()?;
        (arr, typ).try_into()
    }
}

// Implement a custom PartialEq for PointArray to allow Point(EMPTY) comparisons, which is stored
// as (NaN, NaN). By default, these resolve to false
impl PartialEq for PointArray {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        // If the coords are already true, don't check for NaNs
        // TODO: maybe only iterate once for perf?
        if self.coords == other.coords {
            return true;
        }

        if self.coords.len() != other.coords.len() {
            return false;
        }

        // TODO: this should check for point equal.
        for point_idx in 0..self.len() {
            let c1 = self.value(point_idx);
            let c2 = other.value(point_idx);
            if !point_eq(&c1, &c2) {
                return false;
            }
        }

        true
    }
}
