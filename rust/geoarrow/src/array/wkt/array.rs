use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{
    Array, ArrayRef, GenericStringArray, LargeStringArray, OffsetSizeTrait, StringArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, WktType};

use crate::array::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32};
use crate::array::SerializedArray;
use crate::datatypes::SerializedType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::IntoArrow;
use crate::ArrayBase;

/// An immutable array of WKT geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<WKT>>` due to the internal validity bitmap.
///
/// This is a wrapper around an Arrow [GenericStringArray], but additionally stores
/// [Metadata] so that we can persist CRS information about the data.
///
/// Refer to [`crate::io::wkt`] for encoding and decoding this array to the native array types.
#[derive(Debug, Clone, PartialEq)]
pub struct WKTArray<O: OffsetSizeTrait> {
    pub(crate) data_type: WktType,
    pub(crate) array: GenericStringArray<O>,
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> WKTArray<O> {
    /// Create a new WKTArray from a StringArray
    pub fn new(array: GenericStringArray<O>, metadata: Arc<Metadata>) -> Self {
        Self {
            data_type: WktType::new(metadata),
            array,
        }
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consume self and access the underlying data.
    pub fn into_inner(self) -> GenericStringArray<O> {
        self.array
    }

    /// Slices this [`WKBArray`] in place.
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

    /// Replace the [`Metadata`] contained in this array.
    pub fn with_metadata(&self, metadata: Arc<Metadata>) -> Self {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        arr
    }
}

impl<O: OffsetSizeTrait> ArrayBase for WKTArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.data_type(O::IS_LARGE)
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type
            .to_field("geometry", true, O::IS_LARGE)
            .into()
    }

    fn extension_name(&self) -> &str {
        WktType::NAME
    }

    fn into_array_ref(self) -> ArrayRef {
        // Recreate a BinaryArray so that we can force it to have geoarrow.wkb extension type
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.data_type.metadata().clone()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.array.len()
    }

    /// Returns the optional validity.
    fn nulls(&self) -> Option<&NullBuffer> {
        self.array.nulls()
    }
}

impl<O: OffsetSizeTrait> SerializedArray for WKTArray<O> {
    fn data_type(&self) -> SerializedType {
        if O::IS_LARGE {
            SerializedType::LargeWKT(self.data_type.clone())
        } else {
            SerializedType::WKT(self.data_type.clone())
        }
    }

    fn with_metadata(&self, metadata: Arc<Metadata>) -> Arc<dyn SerializedArray> {
        Arc::new(self.with_metadata(metadata))
    }

    fn as_ref(&self) -> &dyn SerializedArray {
        self
    }
}

impl<O: OffsetSizeTrait> IntoArrow for WKTArray<O> {
    type ArrowArray = GenericStringArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        GenericStringArray::new(
            self.array.offsets().clone(),
            self.array.values().clone(),
            self.array.nulls().cloned(),
        )
    }
}

impl<O: OffsetSizeTrait> From<GenericStringArray<O>> for WKTArray<O> {
    fn from(value: GenericStringArray<O>) -> Self {
        Self::new(value, Default::default())
    }
}

impl TryFrom<&dyn Array> for WKTArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Utf8 => {
                let downcasted = value.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            DataType::LargeUtf8 => {
                let downcasted = value.as_any().downcast_ref::<LargeStringArray>().unwrap();
                let geom_array: WKTArray<i64> = downcasted.clone().into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for WKTArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Utf8 => {
                let downcasted = value.as_string::<i32>();
                let geom_array: WKTArray<i32> = downcasted.clone().into();
                Ok(geom_array.into())
            }
            DataType::LargeUtf8 => {
                let downcasted = value.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for WKTArray<i32> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
        Ok(arr)
    }
}

impl TryFrom<(&dyn Array, &Field)> for WKTArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
        Ok(arr)
    }
}

impl From<WKTArray<i32>> for WKTArray<i64> {
    fn from(value: WKTArray<i32>) -> Self {
        let metadata = value.metadata();
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Self::new(
            LargeStringArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls),
            metadata,
        )
    }
}

impl TryFrom<WKTArray<i64>> for WKTArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WKTArray<i64>) -> Result<Self> {
        let metadata = value.metadata();
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Ok(Self::new(
            StringArray::new(offsets_buffer_i64_to_i32(&offsets)?, values, nulls),
            metadata,
        ))
    }
}
