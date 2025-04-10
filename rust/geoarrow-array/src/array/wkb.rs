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

/// An immutable array of WKB geometries.
///
/// This is semantically equivalent to `Vec<Option<WKB>>` due to the internal validity bitmap.
///
/// This array implements [`SerializedArray`], not [`NativeArray`]. This means that you'll need to
/// parse the `WkbArray` into a native-typed GeoArrow array (such as
/// [`GeometryArray`][crate::array::GeometryArray]) before using it for computations.
///
/// Refer to [`crate::io::wkb`] for encoding and decoding this array to the native array types.
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

    /// Infer the minimal NativeType that this WkbArray can be casted to.
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

    fn nulls(&self) -> Option<&NullBuffer> {
        self.array.nulls()
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
}

impl<'a, O: OffsetSizeTrait> ArrayAccessor<'a> for WkbArray<O> {
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

impl<O: OffsetSizeTrait> From<(GenericBinaryArray<O>, WkbType)> for WkbArray<O> {
    fn from((value, typ): (GenericBinaryArray<O>, WkbType)) -> Self {
        Self::new(value, typ.metadata().clone())
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
        let typ = field.try_extension_type::<WkbType>()?;
        (arr, typ).try_into()
    }
}

impl TryFrom<(&dyn Array, &Field)> for WkbArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WkbType>()?;
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

#[cfg(test)]
mod test {
    use crate::GeoArrowArray;
    use crate::builder::WKBBuilder;
    use crate::test::point;

    use super::*;

    fn wkb_data<O: OffsetSizeTrait>() -> WkbArray<O> {
        let mut builder = WKBBuilder::new(WkbType::new(Default::default()));
        builder.push_point(Some(&point::p0()));
        builder.push_point(Some(&point::p1()));
        builder.push_point(Some(&point::p2()));
        builder.finish()
    }

    #[test]
    fn parse_dyn_array_i32() {
        let wkb_array = wkb_data::<i32>();
        let array = wkb_array.to_array_ref();
        let field = wkb_array.data_type.to_field("geometry", true, false);
        let wkb_array_retour: WkbArray<i32> = (array.as_ref(), &field).try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_retour);
    }

    #[test]
    fn parse_dyn_array_i64() {
        let wkb_array = wkb_data::<i64>();
        let array = wkb_array.to_array_ref();
        let field = wkb_array.data_type.to_field("geometry", true, false);
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
}
