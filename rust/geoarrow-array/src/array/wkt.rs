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

    /// Replace the [`ArrayMetadata`] contained in this array.
    pub fn with_metadata(&self, metadata: Arc<Metadata>) -> Self {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        arr
    }
}

impl<O: OffsetSizeTrait> GeoArrowArray for WKTArray<O> {
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
            GeoArrowType::LargeWKT(self.data_type.clone())
        } else {
            GeoArrowType::WKT(self.data_type.clone())
        }
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a, O: OffsetSizeTrait> ArrayAccessor<'a> for WKTArray<O> {
    type Item = Wkt<f64>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        let s = unsafe { self.array.value_unchecked(index) };
        Wkt::from_str(s).map_err(GeoArrowError::WktStrError)
    }
}

impl<O: OffsetSizeTrait> IntoArrow for WKTArray<O> {
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

impl<O: OffsetSizeTrait> From<(GenericStringArray<O>, WktType)> for WKTArray<O> {
    fn from((value, typ): (GenericStringArray<O>, WktType)) -> Self {
        Self::new(value, typ.metadata().clone())
    }
}

impl TryFrom<(&dyn Array, WktType)> for WKTArray<i32> {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WktType)) -> Result<Self> {
        match value.data_type() {
            DataType::Utf8 => Ok((value.as_string::<i32>().clone(), typ).into()),
            DataType::LargeUtf8 => {
                let geom_array: WKTArray<i64> = (value.as_string::<i64>().clone(), typ).into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, WktType)> for WKTArray<i64> {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, WktType)) -> Result<Self> {
        match value.data_type() {
            DataType::Utf8 => {
                let geom_array: WKTArray<i32> = (value.as_string::<i32>().clone(), typ).into();
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

impl TryFrom<(&dyn Array, &Field)> for WKTArray<i32> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WktType>()?;
        (arr, typ).try_into()
    }
}

impl TryFrom<(&dyn Array, &Field)> for WKTArray<i64> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WktType>()?;
        (arr, typ).try_into()
    }
}

impl From<WKTArray<i32>> for WKTArray<i64> {
    fn from(value: WKTArray<i32>) -> Self {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        Self {
            data_type: value.data_type,
            array: LargeStringArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls),
        }
    }
}

impl TryFrom<WKTArray<i64>> for WKTArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WKTArray<i64>) -> Result<Self> {
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
    use arrow_array::builder::GenericStringBuilder;

    use crate::GeoArrowArray;
    use crate::test::point;

    use super::*;

    fn wkt_data<O: OffsetSizeTrait>() -> WKTArray<O> {
        let mut builder = GenericStringBuilder::new();

        wkt::to_wkt::write_geometry(&mut builder, &point::p0()).unwrap();
        builder.append_value("");

        wkt::to_wkt::write_geometry(&mut builder, &point::p1()).unwrap();
        builder.append_value("");

        wkt::to_wkt::write_geometry(&mut builder, &point::p2()).unwrap();
        builder.append_value("");

        WKTArray::new(builder.finish(), Default::default())
    }

    #[test]
    fn parse_dyn_array_i32() {
        let wkb_array = wkt_data::<i32>();
        let array = wkb_array.to_array_ref();
        let field = wkb_array.data_type.to_field("geometry", true, false);
        let wkb_array_retour: WKTArray<i32> = (array.as_ref(), &field).try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_retour);
    }

    #[test]
    fn parse_dyn_array_i64() {
        let wkb_array = wkt_data::<i64>();
        let array = wkb_array.to_array_ref();
        let field = wkb_array.data_type.to_field("geometry", true, false);
        let wkb_array_retour: WKTArray<i64> = (array.as_ref(), &field).try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_retour);
    }

    #[test]
    fn convert_i32_to_i64() {
        let wkb_array = wkt_data::<i32>();
        let wkb_array_i64: WKTArray<i64> = wkb_array.clone().into();
        let wkb_array_i32: WKTArray<i32> = wkb_array_i64.clone().try_into().unwrap();

        assert_eq!(wkb_array, wkb_array_i32);
    }

    #[test]
    fn convert_i64_to_i32_to_i64() {
        let wkb_array = wkt_data::<i64>();
        let wkb_array_i32: WKTArray<i32> = wkb_array.clone().try_into().unwrap();
        let wkb_array_i64: WKTArray<i64> = wkb_array_i32.clone().into();

        assert_eq!(wkb_array, wkb_array_i64);
    }
}
