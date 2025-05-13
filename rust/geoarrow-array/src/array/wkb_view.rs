use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BinaryViewArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, WkbType};
use wkb::reader::Wkb;

use crate::error::{GeoArrowError, Result};
use crate::{ArrayAccessor, GeoArrowArray, GeoArrowType, IntoArrow};

/// An immutable array of WKB geometries.
///
/// This is semantically equivalent to `Vec<Option<Wkb>>` due to the internal validity bitmap.
///
/// This is stored as an Arrow [`BinaryViewArray`]
#[derive(Debug, Clone, PartialEq)]
pub struct WkbViewArray {
    pub(crate) data_type: WkbType,
    pub(crate) array: BinaryViewArray,
}

impl WkbViewArray {
    /// Create a new WkbArray from a BinaryArray
    pub fn new(array: BinaryViewArray, metadata: Arc<Metadata>) -> Self {
        Self {
            data_type: WkbType::new(metadata),
            array,
        }
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Slices this [`WkbArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        Self {
            array: self.array.slice(offset, length),
            data_type: self.data_type.clone(),
        }
    }

    /// Replace the [ArrayMetadata] in the array with the given metadata
    pub fn with_metadata(&self, metadata: Arc<Metadata>) -> Self {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        arr
    }
}

impl GeoArrowArray for WkbViewArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.array.logical_nulls()
    }

    #[inline]
    fn logical_null_count(&self) -> usize {
        self.array.logical_null_count()
    }

    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.array.is_null(i)
    }

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::WkbView(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(Self::with_metadata(&self, metadata))
    }
}

impl<'a> ArrayAccessor<'a> for WkbViewArray {
    type Item = Wkb<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        let buf = self.array.value(index);
        Ok(Wkb::try_new(buf)?)
    }
}

impl IntoArrow for WkbViewArray {
    type ArrowArray = BinaryViewArray;
    type ExtensionType = WkbType;

    fn into_arrow(self) -> Self::ArrowArray {
        self.array
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl From<(BinaryViewArray, WkbType)> for WkbViewArray {
    fn from((value, typ): (BinaryViewArray, WkbType)) -> Self {
        Self {
            data_type: typ,
            array: value,
        }
    }
}

impl TryFrom<(&dyn Array, WkbType)> for WkbViewArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WkbType)) -> Result<Self> {
        match value.data_type() {
            DataType::BinaryView => Ok((value.as_binary_view().clone(), typ).into()),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for WkbViewArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field
            .try_extension_type::<WkbType>()
            .ok()
            .unwrap_or_default();
        (arr, typ).try_into()
    }
}
