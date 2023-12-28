//! Parse non-geometry property values into Arrow arrays

// Inspired by polars
// https://github.com/pola-rs/polars/blob/main/crates/polars-core/src/frame/row/av_buffer.rs#L12

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, Int8Builder, StringBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use arrow_array::Array;
use arrow_schema::{DataType, TimeUnit};
use geozero::ColumnValue;

use crate::error::Result;

// Types implemented by FlatGeobuf
#[derive(Debug)]
pub enum AnyBuilder {
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

impl AnyBuilder {
    /// Row index is the current row index. So a value with no previously-missed values would have
    /// row_index 0.
    pub fn from_value_prefill(value: &ColumnValue, row_index: usize) -> Self {
        match value {
            ColumnValue::Bool(val) => {
                let mut builder = BooleanBuilder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Bool(builder)
            }
            ColumnValue::Byte(val) => {
                let mut builder = Int8Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Int8(builder)
            }
            ColumnValue::UByte(val) => {
                let mut builder = UInt8Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Uint8(builder)
            }
            ColumnValue::Short(val) => {
                let mut builder = Int16Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Int16(builder)
            }
            ColumnValue::UShort(val) => {
                let mut builder = UInt16Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Uint16(builder)
            }
            ColumnValue::Int(val) => {
                let mut builder = Int32Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Int32(builder)
            }
            ColumnValue::UInt(val) => {
                let mut builder = UInt32Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Uint32(builder)
            }
            ColumnValue::Long(val) => {
                let mut builder = Int64Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Int64(builder)
            }
            ColumnValue::ULong(val) => {
                let mut builder = UInt64Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Uint64(builder)
            }
            ColumnValue::Float(val) => {
                let mut builder = Float32Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Float32(builder)
            }
            ColumnValue::Double(val) => {
                let mut builder = Float64Builder::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*val);
                AnyBuilder::Float64(builder)
            }
            ColumnValue::String(val) => {
                let mut builder = StringBuilder::with_capacity(row_index + 1, val.len());
                for _ in 0..row_index {
                    builder.append_null();
                }
                builder.append_value(*val);
                AnyBuilder::String(builder)
            }
            ColumnValue::Json(val) => {
                let mut builder = StringBuilder::with_capacity(row_index + 1, val.len());
                for _ in 0..row_index {
                    builder.append_null();
                }
                builder.append_value(*val);
                AnyBuilder::Json(builder)
            }
            ColumnValue::DateTime(val) => {
                let mut builder = StringBuilder::with_capacity(row_index + 1, val.len());
                for _ in 0..row_index {
                    builder.append_null();
                }
                builder.append_value(*val);
                AnyBuilder::DateTime(builder)
            }
            ColumnValue::Binary(val) => {
                let mut builder = BinaryBuilder::with_capacity(row_index + 1, val.len());
                for _ in 0..row_index {
                    builder.append_null();
                }
                builder.append_value(*val);
                AnyBuilder::Binary(builder)
            }
        }
    }

    pub fn add_value(&mut self, value: &ColumnValue) {
        match (self, value) {
            (AnyBuilder::Bool(arr), ColumnValue::Bool(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Int8(arr), ColumnValue::Byte(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Uint8(arr), ColumnValue::UByte(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Int16(arr), ColumnValue::Short(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Uint16(arr), ColumnValue::UShort(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Int32(arr), ColumnValue::Int(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Uint32(arr), ColumnValue::UInt(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Int64(arr), ColumnValue::Long(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Uint64(arr), ColumnValue::ULong(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Float32(arr), ColumnValue::Float(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Float64(arr), ColumnValue::Double(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::String(arr), ColumnValue::String(val)) => {
                arr.append_value(val);
            }
            (AnyBuilder::Json(arr), ColumnValue::Json(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::DateTime(arr), ColumnValue::DateTime(val)) => {
                arr.append_value(*val);
            }
            (AnyBuilder::Binary(arr), ColumnValue::Binary(val)) => {
                arr.append_value(*val);
            }
            // Should be unreachable
            (s, v) => panic!(
                "Trying to insert a column value {} in the wrong type column {:?}",
                v, s
            ),
        }
    }

    pub fn append_null(&mut self) {
        use AnyBuilder::*;
        match self {
            Bool(arr) => arr.append_null(),
            Int8(arr) => arr.append_null(),
            Uint8(arr) => arr.append_null(),
            Int16(arr) => arr.append_null(),
            Uint16(arr) => arr.append_null(),
            Int32(arr) => arr.append_null(),
            Uint32(arr) => arr.append_null(),
            Int64(arr) => arr.append_null(),
            Uint64(arr) => arr.append_null(),
            Float32(arr) => arr.append_null(),
            Float64(arr) => arr.append_null(),
            String(arr) => arr.append_null(),
            Json(arr) => arr.append_null(),
            DateTime(arr) => arr.append_null(),
            Binary(arr) => arr.append_null(),
        }
    }

    pub fn len(&self) -> usize {
        use AnyBuilder::*;
        match self {
            Bool(arr) => arr.len(),
            Int8(arr) => arr.len(),
            Uint8(arr) => arr.len(),
            Int16(arr) => arr.len(),
            Uint16(arr) => arr.len(),
            Int32(arr) => arr.len(),
            Uint32(arr) => arr.len(),
            Int64(arr) => arr.len(),
            Uint64(arr) => arr.len(),
            Float32(arr) => arr.len(),
            Float64(arr) => arr.len(),
            String(arr) => arr.len(),
            Json(arr) => arr.len(),
            DateTime(arr) => arr.len(),
            Binary(arr) => arr.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(self) -> Result<Arc<dyn Array>> {
        use AnyBuilder::*;
        let arr: Arc<dyn Array> = match self {
            Bool(arr) => Arc::new(arr.finish_cloned()),
            Int8(arr) => Arc::new(arr.finish_cloned()),
            Uint8(arr) => Arc::new(arr.finish_cloned()),
            Int16(arr) => Arc::new(arr.finish_cloned()),
            Uint16(arr) => Arc::new(arr.finish_cloned()),
            Int32(arr) => Arc::new(arr.finish_cloned()),
            Uint32(arr) => Arc::new(arr.finish_cloned()),
            Int64(arr) => Arc::new(arr.finish_cloned()),
            Uint64(arr) => Arc::new(arr.finish_cloned()),
            Float32(arr) => Arc::new(arr.finish_cloned()),
            Float64(arr) => Arc::new(arr.finish_cloned()),
            String(arr) => Arc::new(arr.finish_cloned()),
            Json(arr) => Arc::new(arr.finish_cloned()),
            // TODO: how to support timezones? Or is this always naive tz?
            DateTime(arr) => {
                let string_arr = arr.finish_cloned();
                arrow_cast::cast(
                    &string_arr,
                    &DataType::Timestamp(TimeUnit::Microsecond, None),
                )?
            }
            Binary(arr) => Arc::new(arr.finish_cloned()),
        };
        Ok(arr)
    }
}

macro_rules! impl_from {
    ($from_ty:ty, $variant:expr) => {
        impl From<$from_ty> for AnyBuilder {
            fn from(value: $from_ty) -> Self {
                $variant(value)
            }
        }
    };
}

impl_from!(BooleanBuilder, AnyBuilder::Bool);
impl_from!(Int8Builder, AnyBuilder::Int8);
impl_from!(UInt8Builder, AnyBuilder::Uint8);
impl_from!(Int16Builder, AnyBuilder::Int16);
impl_from!(UInt16Builder, AnyBuilder::Uint16);
impl_from!(Int32Builder, AnyBuilder::Int32);
impl_from!(UInt32Builder, AnyBuilder::Uint32);
impl_from!(Int64Builder, AnyBuilder::Int64);
impl_from!(UInt64Builder, AnyBuilder::Uint64);
impl_from!(Float32Builder, AnyBuilder::Float32);
impl_from!(Float64Builder, AnyBuilder::Float64);
impl_from!(BinaryBuilder, AnyBuilder::Binary);
