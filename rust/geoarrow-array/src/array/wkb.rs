use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, GenericBinaryArray, LargeBinaryArray, OffsetSizeTrait,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{CoordType, Metadata, WkbType};
use wkb::reader::Wkb;

use crate::capacity::WKBCapacity;
use crate::datatypes::GeoArrowType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};
use crate::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32};

/// An immutable array of WKB geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<WKB>>` due to the internal validity bitmap.
///
/// This array implements [`SerializedArray`], not [`NativeArray`]. This means that you'll need to
/// parse the `WKBArray` into a native-typed GeoArrow array (such as
/// [`GeometryArray`][crate::array::GeometryArray]) before using it for computations.
///
/// Refer to [`crate::io::wkb`] for encoding and decoding this array to the native array types.
#[derive(Debug, Clone, PartialEq)]
pub struct WKBArray<O: OffsetSizeTrait> {
    pub(crate) data_type: WkbType,
    pub(crate) array: GenericBinaryArray<O>,
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> WKBArray<O> {
    /// Create a new WKBArray from a BinaryArray
    pub fn new(array: GenericBinaryArray<O>, metadata: Arc<Metadata>) -> Self {
        Self {
            data_type: WkbType::new(metadata),
            array,
        }
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Infer the minimal NativeType that this WKBArray can be casted to.
    #[allow(dead_code)]
    // TODO: is this obsolete with new from_wkb approach that uses downcasting?
    pub(crate) fn infer_geo_data_type(&self, _coord_type: CoordType) -> Result<GeoArrowType> {
        todo!()
        // use crate::io::wkb::reader::r#type::infer_geometry_type;
        // infer_geometry_type(self.iter().flatten(), coord_type)
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> WKBCapacity {
        WKBCapacity::new(
            self.array.offsets().last().unwrap().to_usize().unwrap(),
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes::<O>()
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

    /// Replace the [ArrayMetadata] in the array with the given metadata
    pub fn with_metadata(&self, metadata: Arc<Metadata>) -> Self {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        arr
    }
}

impl<O: OffsetSizeTrait> GeoArrowArray for WKBArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        // Recreate a BinaryArray so that we can force it to have geoarrow.wkb extension type
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
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

    fn data_type(&self) -> GeoArrowType {
        if O::IS_LARGE {
            GeoArrowType::LargeWKB(self.data_type.clone())
        } else {
            GeoArrowType::WKB(self.data_type.clone())
        }
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a, O: OffsetSizeTrait> ArrayAccessor<'a> for WKBArray<O> {
    type Item = Wkb<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        let buf = self.array.value(index);
        Ok(Wkb::try_new(buf)?)
    }
}

impl<O: OffsetSizeTrait> IntoArrow for WKBArray<O> {
    type ArrowArray = GenericBinaryArray<O>;
    type ExtensionType = WkbType;

    fn into_arrow(self) -> Self::ArrowArray {
        GenericBinaryArray::new(
            self.array.offsets().clone(),
            self.array.values().clone(),
            self.array.nulls().cloned(),
        )
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl<O: OffsetSizeTrait> From<GenericBinaryArray<O>> for WKBArray<O> {
    fn from(value: GenericBinaryArray<O>) -> Self {
        Self::new(value, Default::default())
    }
}

impl TryFrom<&dyn Array> for WKBArray<i32> {
    type Error = GeoArrowError;
    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => {
                let downcasted = value.as_any().downcast_ref::<BinaryArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            DataType::LargeBinary => {
                let downcasted = value.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                let geom_array: WKBArray<i64> = downcasted.clone().into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for WKBArray<i64> {
    type Error = GeoArrowError;
    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => {
                let downcasted = value.as_binary::<i32>();
                let geom_array: WKBArray<i32> = downcasted.clone().into();
                Ok(geom_array.into())
            }
            DataType::LargeBinary => {
                let downcasted = value.as_binary::<i64>();
                Ok(downcasted.clone().into())
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for WKBArray<i32> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
        Ok(arr)
    }
}

impl TryFrom<(&dyn Array, &Field)> for WKBArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
        Ok(arr)
    }
}

impl From<WKBArray<i32>> for WKBArray<i64> {
    fn from(value: WKBArray<i32>) -> Self {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        let array = LargeBinaryArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls);
        Self {
            data_type: value.data_type,
            array,
        }
    }
}

impl TryFrom<WKBArray<i64>> for WKBArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<i64>) -> Result<Self> {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        let array = BinaryArray::new(offsets_buffer_i64_to_i32(&offsets)?, values, nulls);
        Ok(Self {
            data_type: value.data_type,
            array,
        })
    }
}
