//! Parse non-geometry property values into Arrow arrays

// Inspired by polars
// https://github.com/pola-rs/polars/blob/main/crates/polars-core/src/frame/row/av_buffer.rs#L12

use arrow2::array::{
    MutableBinaryValuesArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8ValuesArray,
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
}
