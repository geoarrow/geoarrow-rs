//! Parse non-geometry property values into Arrow arrays

// Inspired by polars
// https://github.com/pola-rs/polars/blob/main/crates/polars-core/src/frame/row/av_buffer.rs#L12

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, PrimitiveBuilder, StringBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use arrow_array::{Array, BinaryArray, BooleanArray, PrimitiveArray, StringArray};
use geozero::ColumnValue;

// Types implemented by FlatGeobuf
#[derive(Debug, Clone)]
pub enum AnyMutableArray {
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Uint8(UInt8Builder),
    Int16(Int16Builder),
    Uint16(UInt16Builder),
    Int32(Int32Builder),
    Uint32(UInt32Builder),
    Int64(Int64Builder),
    Uint64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Json(StringBuilder),
    // Note: this gets parsed to a datetime array at the end
    DateTime(StringBuilder),
    Binary(BinaryBuilder),
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
            (s, v) => panic!(
                "Trying to insert a column value {} in the wrong type column {:?}",
                v, s
            ),
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
                let arr: StringArray = arr.into();
                arr.boxed()
            }
            Json(arr) => {
                let arr: StringArray = arr.into();
                arr.boxed()
            }
            // TODO: how to support timezones? Or is this always naive tz?
            DateTime(arr) => todo!(), // arrow2::compute::cast::utf8_to_naive_timestamp_ns(&arr.into()).boxed(),
            Binary(arr) => {
                let arr: BinaryArray = arr.into();
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

impl_from!(BooleanBuilder, AnyMutableArray::Bool);
impl_from!(PrimitiveBuilder<i8>, AnyMutableArray::Int8);
impl_from!(PrimitiveBuilder<u8>, AnyMutableArray::Uint8);
impl_from!(PrimitiveBuilder<i16>, AnyMutableArray::Int16);
impl_from!(PrimitiveBuilder<u16>, AnyMutableArray::Uint16);
impl_from!(PrimitiveBuilder<i32>, AnyMutableArray::Int32);
impl_from!(PrimitiveBuilder<u32>, AnyMutableArray::Uint32);
impl_from!(PrimitiveBuilder<i64>, AnyMutableArray::Int64);
impl_from!(PrimitiveBuilder<u64>, AnyMutableArray::Uint64);
impl_from!(PrimitiveBuilder<f32>, AnyMutableArray::Float32);
impl_from!(PrimitiveBuilder<f64>, AnyMutableArray::Float64);
impl_from!(BinaryBuilder, AnyMutableArray::Binary);
