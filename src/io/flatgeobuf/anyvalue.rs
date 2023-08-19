//! Parse non-geometry property values into Arrow arrays

// Inspired by polars
// https://github.com/pola-rs/polars/blob/main/crates/polars-core/src/frame/row/av_buffer.rs#L12

use arrow2::array::{
    Array, BinaryArray, BooleanArray, MutableBinaryValuesArray, MutableBooleanArray,
    MutablePrimitiveArray, MutableUtf8ValuesArray, PrimitiveArray, Utf8Array,
};
use geozero::ColumnValue;

// Types implemented by FlatGeobuf
pub enum AnyMutableArray {
    Bool(MutableBooleanArray),
    Int8(MutablePrimitiveArray<i8>),
    Uint8(MutablePrimitiveArray<u8>),
    Int16(MutablePrimitiveArray<i16>),
    Uint16(MutablePrimitiveArray<u16>),
    Int32(MutablePrimitiveArray<i32>),
    Uint32(MutablePrimitiveArray<u32>),
    Int64(MutablePrimitiveArray<i64>),
    Uint64(MutablePrimitiveArray<u64>),
    Float32(MutablePrimitiveArray<f32>),
    Float64(MutablePrimitiveArray<f64>),
    String(MutableUtf8ValuesArray<i32>),
    Json(MutableUtf8ValuesArray<i32>),
    // Note: this gets parsed to a datetime array at the end
    DateTime(MutableUtf8ValuesArray<i32>),
    Binary(MutableBinaryValuesArray<i32>),
}

impl AnyMutableArray {
    pub fn add_value(&mut self, value: &ColumnValue) {
        match (self, value) {
            (AnyMutableArray::Bool(arr), ColumnValue::Bool(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Int8(arr), ColumnValue::Byte(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Uint8(arr), ColumnValue::UByte(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Int16(arr), ColumnValue::Short(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Uint16(arr), ColumnValue::UShort(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Int32(arr), ColumnValue::Int(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Uint32(arr), ColumnValue::UInt(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Int64(arr), ColumnValue::Long(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Uint64(arr), ColumnValue::ULong(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Float32(arr), ColumnValue::Float(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::Float64(arr), ColumnValue::Double(val)) => {
                arr.push(Some(*val));
            }
            (AnyMutableArray::String(arr), ColumnValue::String(val)) => {
                arr.push(val);
            }
            (AnyMutableArray::Json(arr), ColumnValue::Json(val)) => {
                arr.push(*val);
            }
            (AnyMutableArray::DateTime(arr), ColumnValue::DateTime(val)) => {
                arr.push(*val);
            }
            (AnyMutableArray::Binary(arr), ColumnValue::Binary(val)) => {
                arr.push(*val);
            }
            // Should be unreachable
            _ => panic!("Trying to insert a column value in the wrong type column"),
        }
    }

    pub fn finish(self) -> Box<dyn Array> {
        use AnyMutableArray::*;
        match self {
            Bool(arr) => BooleanArray::from(arr).boxed(),
            Int8(arr) => PrimitiveArray::from(arr).boxed(),
            Uint8(arr) => PrimitiveArray::from(arr).boxed(),
            Int16(arr) => PrimitiveArray::from(arr).boxed(),
            Uint16(arr) => PrimitiveArray::from(arr).boxed(),
            Int32(arr) => PrimitiveArray::from(arr).boxed(),
            Uint32(arr) => PrimitiveArray::from(arr).boxed(),
            Int64(arr) => PrimitiveArray::from(arr).boxed(),
            Uint64(arr) => PrimitiveArray::from(arr).boxed(),
            Float32(arr) => PrimitiveArray::from(arr).boxed(),
            Float64(arr) => PrimitiveArray::from(arr).boxed(),
            String(arr) => {
                let arr: Utf8Array<i32> = arr.into();
                arr.boxed()
            }
            Json(arr) => {
                let arr: Utf8Array<i32> = arr.into();
                arr.boxed()
            }
            // TODO: convert datetime columns to timestamps
            // https://docs.rs/arrow2/latest/arrow2/temporal_conversions/fn.utf8_to_naive_timestamp_scalar.html
            DateTime(_arr) => todo!(),
            Binary(arr) => {
                let arr: BinaryArray<i32> = arr.into();
                arr.boxed()
            }
        }
    }
}

macro_rules! impl_from {
    ($from_ty:ty, $variant:expr) => {
        impl From<$from_ty> for AnyMutableArray {
            fn from(value: $from_ty) -> Self {
                $variant(value)
            }
        }
    };
}

impl_from!(MutableBooleanArray, AnyMutableArray::Bool);
impl_from!(MutablePrimitiveArray<i8>, AnyMutableArray::Int8);
impl_from!(MutablePrimitiveArray<u8>, AnyMutableArray::Uint8);
impl_from!(MutablePrimitiveArray<i16>, AnyMutableArray::Int16);
impl_from!(MutablePrimitiveArray<u16>, AnyMutableArray::Uint16);
impl_from!(MutablePrimitiveArray<i32>, AnyMutableArray::Int32);
impl_from!(MutablePrimitiveArray<u32>, AnyMutableArray::Uint32);
impl_from!(MutablePrimitiveArray<i64>, AnyMutableArray::Int64);
impl_from!(MutablePrimitiveArray<u64>, AnyMutableArray::Uint64);
impl_from!(MutablePrimitiveArray<f32>, AnyMutableArray::Float32);
impl_from!(MutablePrimitiveArray<f64>, AnyMutableArray::Float64);
impl_from!(MutableBinaryValuesArray<i32>, AnyMutableArray::Binary);
