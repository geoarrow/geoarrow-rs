use std::sync::Arc;

use arrow_array::builder::GenericByteBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, GenericBinaryArray, LargeBinaryArray, OffsetSizeTrait,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, WkbType};
use wkb::reader::Wkb;

use crate::array::WkbViewArray;
use crate::capacity::WkbCapacity;
use crate::datatypes::GeoArrowType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow};
use crate::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32};

/// An immutable array of WKB geometries.
///
/// This is semantically equivalent to `Vec<Option<Wkb>>` due to the internal validity bitmap.
///
/// This is stored either as an Arrow [`BinaryArray`] or [`LargeBinaryArray`].
#[derive(Debug, Clone, PartialEq)]
pub struct WkbArray<O: OffsetSizeTrait> {
    pub(crate) data_type: WkbType,
    pub(crate) array: GenericBinaryArray<O>,
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> WkbArray<O> {
    /// Create a new WkbArray from a BinaryArray
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

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> WkbCapacity {
        WkbCapacity::new(
            self.array.offsets().last().unwrap().to_usize().unwrap(),
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self
            .array
            .nulls()
            .as_ref()
            .map(|v| v.buffer().len())
            .unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes::<O>()
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

impl<O: OffsetSizeTrait> GeoArrowArray for WkbArray<O> {
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
        if O::IS_LARGE {
            GeoArrowType::LargeWkb(self.data_type.clone())
        } else {
            GeoArrowType::Wkb(self.data_type.clone())
        }
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(Self::with_metadata(&self, metadata))
    }
}

impl<'a, O: OffsetSizeTrait> GeoArrowArrayAccessor<'a> for WkbArray<O> {
    type Item = Wkb<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        let buf = self.array.value(index);
        Ok(Wkb::try_new(buf)?)
    }
}

impl<O: OffsetSizeTrait> IntoArrow for WkbArray<O> {
    type ArrowArray = GenericBinaryArray<O>;
    type ExtensionType = WkbType;

    fn into_arrow(self) -> Self::ArrowArray {
        self.array
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl<O: OffsetSizeTrait> From<(GenericBinaryArray<O>, WkbType)> for WkbArray<O> {
    fn from((value, typ): (GenericBinaryArray<O>, WkbType)) -> Self {
        Self {
            data_type: typ,
            array: value,
        }
    }
}

impl TryFrom<(&dyn Array, WkbType)> for WkbArray<i32> {
    type Error = GeoArrowError;
    fn try_from((value, typ): (&dyn Array, WkbType)) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => Ok((value.as_binary::<i32>().clone(), typ).into()),
            DataType::LargeBinary => {
                let geom_array: WkbArray<i64> = (value.as_binary::<i64>().clone(), typ).into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, WkbType)> for WkbArray<i64> {
    type Error = GeoArrowError;
    fn try_from((value, typ): (&dyn Array, WkbType)) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => {
                let geom_array: WkbArray<i32> = (value.as_binary::<i32>().clone(), typ).into();
                Ok(geom_array.into())
            }
            DataType::LargeBinary => Ok((value.as_binary::<i64>().clone(), typ).into()),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for WkbArray<i32> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field
            .try_extension_type::<WkbType>()
            .ok()
            .unwrap_or_default();
        (arr, typ).try_into()
    }
}

impl TryFrom<(&dyn Array, &Field)> for WkbArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field
            .try_extension_type::<WkbType>()
            .ok()
            .unwrap_or_default();
        (arr, typ).try_into()
    }
}

impl From<WkbArray<i32>> for WkbArray<i64> {
    fn from(value: WkbArray<i32>) -> Self {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        let array = LargeBinaryArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls);
        Self {
            data_type: value.data_type,
            array,
        }
    }
}

impl TryFrom<WkbArray<i64>> for WkbArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WkbArray<i64>) -> Result<Self> {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        let array = BinaryArray::new(offsets_buffer_i64_to_i32(&offsets)?, values, nulls);
        Ok(Self {
            data_type: value.data_type,
            array,
        })
    }
}

impl<O: OffsetSizeTrait> From<WkbViewArray> for WkbArray<O> {
    fn from(value: WkbViewArray) -> Self {
        let wkb_type = value.data_type;
        let binary_view_array = value.array;

        // Copy the bytes from the binary view array into a new byte array
        let mut builder = GenericByteBuilder::new();
        binary_view_array
            .iter()
            .for_each(|value| builder.append_option(value));

        Self {
            data_type: wkb_type,
            array: builder.finish(),
        }
    }
}

#[cfg(test)]
mod test {
    use arrow_array::builder::{BinaryBuilder, LargeBinaryBuilder};

    use crate::GeoArrowArray;
    use crate::builder::WkbBuilder;
    use crate::test::point;

    use super::*;

    fn wkb_data<O: OffsetSizeTrait>() -> WkbArray<O> {
        let mut builder = WkbBuilder::new(WkbType::new(Default::default()));
        builder.push_point(Some(&point::p0()));
        builder.push_point(Some(&point::p1()));
        builder.push_point(Some(&point::p2()));
        builder.finish()
    }

    #[test]
    fn parse_dyn_array_i32() {
        let wkb_array = wkb_data::<i32>();
        let array = wkb_array.to_array_ref();
        let field = Field::new("geometry", array.data_type().clone(), true)
            .with_extension_type(wkb_array.data_type.clone());
        let wkb_array_retour: WkbArray<i32> = (array.as_ref(), &field).try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_retour);
    }

    #[test]
    fn parse_dyn_array_i64() {
        let wkb_array = wkb_data::<i64>();
        let array = wkb_array.to_array_ref();
        let field = Field::new("geometry", array.data_type().clone(), true)
            .with_extension_type(wkb_array.data_type.clone());
        let wkb_array_retour: WkbArray<i64> = (array.as_ref(), &field).try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_retour);
    }

    #[test]
    fn convert_i32_to_i64() {
        let wkb_array = wkb_data::<i32>();
        let wkb_array_i64: WkbArray<i64> = wkb_array.clone().into();
        let wkb_array_i32: WkbArray<i32> = wkb_array_i64.clone().try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_i32);
    }

    #[test]
    fn convert_i64_to_i32_to_i64() {
        let wkb_array = wkb_data::<i64>();
        let wkb_array_i32: WkbArray<i32> = wkb_array.clone().try_into().unwrap();
        let wkb_array_i64: WkbArray<i64> = wkb_array_i32.clone().into();

        assert_eq!(wkb_array, wkb_array_i64);
    }

    /// Passing a field without an extension name should not panic
    #[test]
    fn allow_field_without_extension_name() {
        // String array
        let mut builder = BinaryBuilder::new();
        builder.append_value(b"a");
        let array = Arc::new(builder.finish()) as ArrayRef;
        let field = Field::new("geometry", array.data_type().clone(), true);
        let _wkt_arr = WkbArray::<i32>::try_from((array.as_ref(), &field)).unwrap();

        // Large string
        let mut builder = LargeBinaryBuilder::new();
        builder.append_value(b"a");
        let array = Arc::new(builder.finish()) as ArrayRef;
        let field = Field::new("geometry", array.data_type().clone(), true);
        let _wkt_arr = WkbArray::<i64>::try_from((array.as_ref(), &field)).unwrap();
    }
}
