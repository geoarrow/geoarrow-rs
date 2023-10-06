//! Parse non-geometry property values into Arrow arrays

// Inspired by polars
// https://github.com/pola-rs/polars/blob/main/crates/polars-core/src/frame/row/av_buffer.rs#L12

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
    UInt8Builder,
};
use arrow_array::Array;
use geozero::ColumnValue;

// Types implemented by FlatGeobuf
#[derive(Debug)]
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
                arr.append_value(*val);
            }
            (AnyMutableArray::Int8(arr), ColumnValue::Byte(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Uint8(arr), ColumnValue::UByte(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Int16(arr), ColumnValue::Short(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Uint16(arr), ColumnValue::UShort(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Int32(arr), ColumnValue::Int(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Uint32(arr), ColumnValue::UInt(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Int64(arr), ColumnValue::Long(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Uint64(arr), ColumnValue::ULong(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Float32(arr), ColumnValue::Float(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Float64(arr), ColumnValue::Double(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::String(arr), ColumnValue::String(val)) => {
                arr.append_value(val);
            }
            (AnyMutableArray::Json(arr), ColumnValue::Json(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::DateTime(arr), ColumnValue::DateTime(val)) => {
                arr.append_value(*val);
            }
            (AnyMutableArray::Binary(arr), ColumnValue::Binary(val)) => {
                arr.append_value(*val);
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
            Bool(arr) => Box::new(arr.finish_cloned()),
            Int8(arr) => Box::new(arr.finish_cloned()),
            Uint8(arr) => Box::new(arr.finish_cloned()),
            Int16(arr) => Box::new(arr.finish_cloned()),
            Uint16(arr) => Box::new(arr.finish_cloned()),
            Int32(arr) => Box::new(arr.finish_cloned()),
            Uint32(arr) => Box::new(arr.finish_cloned()),
            Int64(arr) => Box::new(arr.finish_cloned()),
            Uint64(arr) => Box::new(arr.finish_cloned()),
            Float32(arr) => Box::new(arr.finish_cloned()),
            Float64(arr) => Box::new(arr.finish_cloned()),
            String(arr) => Box::new(arr.finish_cloned()),
            Json(arr) => Box::new(arr.finish_cloned()),
            // TODO: how to support timezones? Or is this always naive tz?
            DateTime(arr) => todo!(), // arrow2::compute::cast::utf8_to_naive_timestamp_ns(&arr.into()).boxed(),
            Binary(arr) => Box::new(arr.finish_cloned()),
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
impl_from!(Int8Builder, AnyMutableArray::Int8);
impl_from!(UInt8Builder, AnyMutableArray::Uint8);
impl_from!(Int16Builder, AnyMutableArray::Int16);
impl_from!(UInt16Builder, AnyMutableArray::Uint16);
impl_from!(Int32Builder, AnyMutableArray::Int32);
impl_from!(UInt32Builder, AnyMutableArray::Uint32);
impl_from!(Int64Builder, AnyMutableArray::Int64);
impl_from!(UInt64Builder, AnyMutableArray::Uint64);
impl_from!(Float32Builder, AnyMutableArray::Float32);
impl_from!(Float64Builder, AnyMutableArray::Float64);
impl_from!(BinaryBuilder, AnyMutableArray::Binary);
