use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::iterator::ArrayIter;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, GenericBinaryArray, LargeBinaryArray,
    OffsetSizeTrait,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, WkbType};
use wkb::reader::Wkb;

use crate::capacity::WkbCapacity;
use crate::datatypes::GeoArrowType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};
use crate::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32};

/// An immutable array of WKB geometries.
///
/// This is semantically equivalent to `Vec<Option<Wkb>>` due to the internal validity bitmap.
///
/// This array implements [`SerializedArray`], not [`NativeArray`]. This means that you'll need to
/// parse the `WkbArray` into a native-typed GeoArrow array (such as
/// [`GeometryArray`][crate::array::GeometryArray]) before using it for computations.
///
/// Refer to [`crate::io::wkb`] for encoding and decoding this array to the native array types.
#[derive(Debug, Clone, PartialEq)]
pub struct WkbArray<A: GeoArrowBinaryArrayType> {
    pub(crate) data_type: WkbType,
    pub(crate) array: A,
}

// Implement geometry accessors
impl<A: GeoArrowBinaryArrayType> WkbArray<A> {
    /// Create a new [WkbArray] from a [BinaryArray], [LargeBinaryArray], or [BinaryViewArray]
    pub fn new(array: A, metadata: Arc<Metadata>) -> Self {
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

impl<A: GeoArrowBinaryArrayType> GeoArrowArray for WkbArray<A> {
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
        match self.array.data_type() {
            DataType::Binary => GeoArrowType::Wkb(self.data_type.clone()),
            DataType::LargeBinary => GeoArrowType::LargeWkb(self.data_type.clone()),
            DataType::BinaryView => GeoArrowType::WkbView(self.data_type.clone()),
            dt => panic!("Unexpected type: {:?}", dt),
        }
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }

    fn with_metadata(self, metadata: Arc<Metadata>) -> Arc<dyn GeoArrowArray> {
        Arc::new(Self::with_metadata(&self, metadata))
    }
}

impl<'a, A: GeoArrowBinaryArrayType + arrow_array::BinaryArrayType<'a>> ArrayAccessor<'a>
    for WkbArray<A>
{
    type Item = Wkb<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        let buf = self.array.value(index);
        Ok(Wkb::try_new(buf)?)
    }
}

impl<A: GeoArrowBinaryArrayType> IntoArrow for WkbArray<A> {
    type ArrowArray = A;
    type ExtensionType = WkbType;

    fn into_arrow(self) -> Self::ArrowArray {
        self.array
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl From<(BinaryArray, WkbType)> for WkbArray<BinaryArray> {
    fn from((value, typ): (BinaryArray, WkbType)) -> Self {
        Self::new(value, typ.metadata().clone())
    }
}

impl From<(LargeBinaryArray, WkbType)> for WkbArray<LargeBinaryArray> {
    fn from((value, typ): (LargeBinaryArray, WkbType)) -> Self {
        Self::new(value, typ.metadata().clone())
    }
}

impl From<(BinaryViewArray, WkbType)> for WkbArray<BinaryViewArray> {
    fn from((value, typ): (BinaryViewArray, WkbType)) -> Self {
        Self::new(value, typ.metadata().clone())
    }
}

impl TryFrom<(&dyn Array, WkbType)> for WkbArray<BinaryArray> {
    type Error = GeoArrowError;
    fn try_from((value, typ): (&dyn Array, WkbType)) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => Ok((value.as_binary::<i32>().clone(), typ).into()),
            DataType::LargeBinary => {
                let geom_array: WkbArray<LargeBinaryArray> =
                    (value.as_binary::<i64>().clone(), typ).into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, WkbType)> for WkbArray<LargeBinaryArray> {
    type Error = GeoArrowError;
    fn try_from((value, typ): (&dyn Array, WkbType)) -> Result<Self> {
        match value.data_type() {
            DataType::Binary => {
                let geom_array: WkbArray<BinaryArray> =
                    (value.as_binary::<i32>().clone(), typ).into();
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

impl TryFrom<(&dyn Array, WkbType)> for WkbArray<BinaryViewArray> {
    type Error = GeoArrowError;
    fn try_from((value, typ): (&dyn Array, WkbType)) -> Result<Self> {
        match value.data_type() {
            // DataType::Binary => {
            //     let geom_array: WkbArray<i32> = (value.as_binary::<i32>().clone(), typ).into();
            //     Ok(geom_array.into())
            // }
            // DataType::LargeBinary => Ok((value.as_binary::<i64>().clone(), typ).into()),
            DataType::BinaryView => Ok((value.as_binary_view().clone(), typ).into()),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for WkbArray<BinaryArray> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WkbType>()?;
        (arr, typ).try_into()
    }
}

impl TryFrom<(&dyn Array, &Field)> for WkbArray<LargeBinaryArray> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WkbType>()?;
        (arr, typ).try_into()
    }
}

impl TryFrom<(&dyn Array, &Field)> for WkbArray<BinaryViewArray> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<WkbType>()?;
        (arr, typ).try_into()
    }
}

impl From<WkbArray<BinaryArray>> for WkbArray<LargeBinaryArray> {
    fn from(value: WkbArray<BinaryArray>) -> Self {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        let array = LargeBinaryArray::new(offsets_buffer_i32_to_i64(&offsets), values, nulls);
        Self {
            data_type: value.data_type,
            array,
        }
    }
}

impl TryFrom<WkbArray<LargeBinaryArray>> for WkbArray<BinaryArray> {
    type Error = GeoArrowError;

    fn try_from(value: WkbArray<LargeBinaryArray>) -> Result<Self> {
        let binary_array = value.array;
        let (offsets, values, nulls) = binary_array.into_parts();
        let array = BinaryArray::new(offsets_buffer_i64_to_i32(&offsets)?, values, nulls);
        Ok(Self {
            data_type: value.data_type,
            array,
        })
    }
}

// https://www.reddit.com/r/rust/comments/12nhpvz/how_can_a_parameter_type_t_be_not_long_living/
trait GeoArrowBinaryArrayType: Array + Clone + Debug + private::Sealed + 'static {}

impl<O: OffsetSizeTrait> GeoArrowBinaryArrayType for GenericBinaryArray<O> {}
impl<'a, O: OffsetSizeTrait> GeoArrowBinaryArrayType for &'a GenericBinaryArray<O> {}
impl GeoArrowBinaryArrayType for BinaryViewArray {}

// /// A trait for Arrow String Arrays, currently three types are supported:
// /// - `BinaryArray`
// /// - `LargeBinaryArray`
// /// - `BinaryViewArray`
// ///
// /// This trait helps to abstract over the different types of binary arrays
// /// so that we don't need to duplicate the implementation for each type.
// trait BinaryArrayType<'a>: arrow_array::ArrayAccessor<Item = &'a [u8]> + Sized {
//     /// Constructs a new iterator
//     fn iter(&self) -> ArrayIter<Self>;
// }

// impl<'a, O: OffsetSizeTrait> BinaryArrayType<'a> for GenericBinaryArray<O> {
//     fn iter(&self) -> ArrayIter<Self> {
//         GenericBinaryArray::<O>::iter(self)
//     }
// }
// impl<'a> BinaryArrayType<'a> for BinaryViewArray {
//     fn iter(&self) -> ArrayIter<Self> {
//         BinaryViewArray::iter(self)
//     }
// }

// https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
mod private {
    use super::*;

    pub trait Sealed {}

    impl<O: OffsetSizeTrait> Sealed for GenericBinaryArray<O> {}
    impl<'a, O: OffsetSizeTrait> Sealed for &'a GenericBinaryArray<O> {}
    impl Sealed for BinaryViewArray {}
}

#[cfg(test)]
mod test {
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
}
