use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{Array, GenericStringArray, LargeStringArray, OffsetSizeTrait, StringArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};

use crate::array::metadata::ArrayMetadata;
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
/// This array _can_ be used directly for operations, but that will incur costly encoding to and
/// from WKT on every operation. Instead, you usually want to use the WKBArray only for
/// serialization purposes (e.g. to and from [GeoParquet](https://geoparquet.org/)) but convert to
/// strongly-typed arrays (such as the [`PointArray`][crate::array::PointArray]) for computations.
#[derive(Debug, Clone, PartialEq)]
pub struct WKTArray<O: OffsetSizeTrait> {
    pub(crate) data_type: SerializedType,
    pub(crate) metadata: Arc<ArrayMetadata>,
    pub(crate) array: GenericStringArray<O>,
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> WKTArray<O> {
    /// Create a new WKTArray from a StringArray
    pub fn new(array: GenericStringArray<O>, metadata: Arc<ArrayMetadata>) -> Self {
        let data_type = match O::IS_LARGE {
            true => SerializedType::LargeWKT,
            false => SerializedType::WKT,
        };

        Self {
            data_type,
            metadata,
            array,
        }
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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
            data_type: self.data_type,
            metadata: self.metadata(),
        }
    }

    pub fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> Self {
        let mut arr = self.clone();
        arr.metadata = metadata;
        arr
    }
}

impl<O: OffsetSizeTrait> ArrayBase for WKTArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.to_data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type
            .to_field_with_metadata("geometry", true, &self.metadata)
            .into()
    }

    fn extension_name(&self) -> &str {
        self.data_type.extension_name()
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        // Recreate a BinaryArray so that we can force it to have geoarrow.wkb extension type
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
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
        self.data_type
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> Arc<dyn SerializedArray> {
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
            DataType::Binary => {
                let downcasted = value.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            DataType::LargeBinary => {
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
            DataType::Binary => {
                let downcasted = value.as_string::<i32>();
                let geom_array: WKTArray<i32> = downcasted.clone().into();
                Ok(geom_array.into())
            }
            DataType::LargeBinary => {
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
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl TryFrom<(&dyn Array, &Field)> for WKTArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl From<WKTArray<i32>> for WKTArray<i64> {
    fn from(value: WKTArray<i32>) -> Self {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Self::new(
            LargeStringArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls),
            value.metadata,
        )
    }
}

impl TryFrom<WKTArray<i64>> for WKTArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WKTArray<i64>) -> Result<Self> {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Ok(Self::new(
            StringArray::new(offsets_buffer_i64_to_i32(&offsets)?, values, nulls),
            value.metadata,
        ))
    }
}
