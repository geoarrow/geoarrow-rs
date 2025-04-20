use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, GenericStringArray, LargeStringArray, OffsetSizeTrait, StringArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, WktType};
use wkt::Wkt;

use crate::ArrayAccessor;
use crate::datatypes::GeoArrowType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{GeoArrowArray, IntoArrow};
use crate::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32};

/// An immutable array of WKT geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<WKT>>` due to the internal validity bitmap.
///
/// This is a wrapper around an Arrow [GenericStringArray], but additionally stores an
/// [ArrayMetadata] so that we can persist CRS information about the data.
///
/// Refer to [`crate::io::wkt`] for encoding and decoding this array to the native array types.
#[derive(Debug, Clone, PartialEq)]
pub struct WktArray<O: OffsetSizeTrait> {
    pub(crate) data_type: WktType,
    pub(crate) array: GenericStringArray<O>,
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> WktArray<O> {
    /// Create a new WktArray from a StringArray
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

    /// Access the underlying string array.
    pub fn inner(&self) -> &GenericStringArray<O> {
        &self.array
    }

    /// Consume self and access the underlying data.
    pub fn into_inner(self) -> GenericStringArray<O> {
        self.array
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

    /// Replace the [`ArrayMetadata`] contained in this array.
    pub fn with_metadata(&self, metadata: Arc<Metadata>) -> Self {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        arr
    }
}

impl<O: OffsetSizeTrait> GeoArrowArray for WktArray<O> {
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
            GeoArrowType::LargeWkt(self.data_type.clone())
        } else {
            GeoArrowType::Wkt(self.data_type.clone())
        }
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a, O: OffsetSizeTrait> ArrayAccessor<'a> for WktArray<O> {
    type Item = Wkt<f64>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        let s = unsafe { self.array.value_unchecked(index) };
        Wkt::from_str(s).map_err(GeoArrowError::WktStrError)
    }
}

impl<O: OffsetSizeTrait> IntoArrow for WktArray<O> {
    type ArrowArray = GenericStringArray<O>;
    type ExtensionType = WktType;

    fn into_arrow(self) -> Self::ArrowArray {
        GenericStringArray::new(
            self.array.offsets().clone(),
            self.array.values().clone(),
            self.array.nulls().cloned(),
        )
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl<O: OffsetSizeTrait> From<(GenericStringArray<O>, WktType)> for WktArray<O> {
    fn from((value, typ): (GenericStringArray<O>, WktType)) -> Self {
        Self::new(value, typ.metadata().clone())
    }
}

impl TryFrom<(&dyn Array, WktType)> for WktArray<i32> {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WktType)) -> Result<Self> {
        match value.data_type() {
            DataType::Utf8 => Ok((value.as_string::<i32>().clone(), typ).into()),
            DataType::LargeUtf8 => {
                let geom_array: WktArray<i64> = (value.as_string::<i64>().clone(), typ).into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, WktType)> for WktArray<i64> {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WktType)) -> Result<Self> {
        match value.data_type() {
            DataType::Utf8 => {
                let geom_array: WktArray<i32> = (value.as_string::<i32>().clone(), typ).into();
                Ok(geom_array.into())
            }
            DataType::LargeUtf8 => Ok((value.as_string::<i64>().clone(), typ).into()),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for WktArray<i32> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WktType>()?;
        (arr, typ).try_into()
    }
}

impl TryFrom<(&dyn Array, &Field)> for WktArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WktType>()?;
        (arr, typ).try_into()
    }
}

impl From<WktArray<i32>> for WktArray<i64> {
    fn from(value: WktArray<i32>) -> Self {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Self {
            data_type: value.data_type,
            array: LargeStringArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls),
        }
    }
}

impl TryFrom<WktArray<i64>> for WktArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WktArray<i64>) -> Result<Self> {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Ok(Self {
            data_type: value.data_type,
            array: StringArray::new(offsets_buffer_i64_to_i32(&offsets)?, values, nulls),
        })
    }
}

#[cfg(test)]
mod test {
    use geoarrow_schema::{CoordType, Dimension};

    use crate::GeoArrowArray;
    use crate::cast::to_wkt;
    use crate::test::point;

    use super::*;

    fn wkt_data<O: OffsetSizeTrait>() -> WktArray<O> {
        to_wkt(&point::array(CoordType::Interleaved, Dimension::XY)).unwrap()
    }

    #[test]
    fn parse_dyn_array_i32() {
        let wkb_array = wkt_data::<i32>();
        let array = wkb_array.to_array_ref();
        let field = wkb_array.data_type.to_field("geometry", true, false);
        let wkb_array_retour: WktArray<i32> = (array.as_ref(), &field).try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_retour);
    }

    #[test]
    fn parse_dyn_array_i64() {
        let wkb_array = wkt_data::<i64>();
        let array = wkb_array.to_array_ref();
        let field = wkb_array.data_type.to_field("geometry", true, false);
        let wkb_array_retour: WktArray<i64> = (array.as_ref(), &field).try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_retour);
    }

    #[test]
    fn convert_i32_to_i64() {
        let wkb_array = wkt_data::<i32>();
        let wkb_array_i64: WktArray<i64> = wkb_array.clone().into();
        let wkb_array_i32: WktArray<i32> = wkb_array_i64.clone().try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_i32);
    }

    #[test]
    fn convert_i64_to_i32_to_i64() {
        let wkb_array = wkt_data::<i64>();
        let wkb_array_i32: WktArray<i32> = wkb_array.clone().try_into().unwrap();
        let wkb_array_i64: WktArray<i64> = wkb_array_i32.clone().into();

        assert_eq!(wkb_array, wkb_array_i64);
    }
}
