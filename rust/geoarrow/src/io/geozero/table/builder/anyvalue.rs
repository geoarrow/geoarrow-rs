//! Parse non-geometry property values into Arrow arrays

// Inspired by polars
// https://github.com/pola-rs/polars/blob/main/crates/polars-core/src/frame/row/av_buffer.rs#L12

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow_cast::parse::string_to_datetime;
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::{DateTime, Utc};
use enum_as_inner::EnumAsInner;
use geozero::ColumnValue;

use crate::error::{GeoArrowError, Result};

// Types implemented by FlatGeobuf/Geozero
/// Builder for a single column's properties
///
/// This holds a contiguous chunk of data. To create an [ArrayRef], call `finish()`.
#[derive(Debug, EnumAsInner)]
pub enum AnyBuilder {
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Uint64(UInt64Builder),
    Uint8(UInt8Builder),
    Uint16(UInt16Builder),
    Uint32(UInt32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Json(StringBuilder),
    Date32(Date32Builder),
    DateTime((TimestampMicrosecondBuilder, Option<Arc<str>>)),
    Binary(BinaryBuilder),
}

impl AnyBuilder {
    /// Create a new builder from a timestamp value at position `i`
    ///
    /// This is a relative hack around the geozero type system because we have an already-parsed
    /// datetime value and geozero only supports string-formatted timestamps.
    #[allow(dead_code)]
    pub(crate) fn from_timestamp_value_prefill(
        value: DateTime<Utc>,
        row_index: usize,
        tz: Option<Arc<str>>,
    ) -> Self {
        let mut builder = TimestampMicrosecondBuilder::with_capacity(row_index + 1);
        for _ in 0..row_index {
            builder.append_null();
        }

        builder.append_value(value.timestamp_micros());
        AnyBuilder::DateTime((builder, tz))
    }

    /// Create a new [AnyBuilder], filling nulls for all values prior to the current `row_index`.
    ///
    /// Row index is the current row index. So a value with no previously-missed values would have
    /// row_index 0. We add 1 so that we have capacity for the current row's value as well.
    pub fn from_value_prefill(value: &ColumnValue, row_index: usize) -> Self {
        macro_rules! impl_prefill {
            ($builder:ty, $val:ident, $variant:expr) => {{
                let mut builder = <$builder>::with_capacity(row_index + 1);
                builder.append_nulls(row_index);
                builder.append_value(*$val);
                $variant(builder)
            }};
        }

        match value {
            ColumnValue::Bool(val) => impl_prefill!(BooleanBuilder, val, AnyBuilder::Bool),
            ColumnValue::Byte(val) => impl_prefill!(Int8Builder, val, AnyBuilder::Int8),
            ColumnValue::UByte(val) => impl_prefill!(UInt8Builder, val, AnyBuilder::Uint8),
            ColumnValue::Short(val) => impl_prefill!(Int16Builder, val, AnyBuilder::Int16),
            ColumnValue::UShort(val) => impl_prefill!(UInt16Builder, val, AnyBuilder::Uint16),
            ColumnValue::Int(val) => impl_prefill!(Int32Builder, val, AnyBuilder::Int32),
            ColumnValue::UInt(val) => impl_prefill!(UInt32Builder, val, AnyBuilder::Uint32),
            ColumnValue::Long(val) => impl_prefill!(Int64Builder, val, AnyBuilder::Int64),
            ColumnValue::ULong(val) => impl_prefill!(UInt64Builder, val, AnyBuilder::Uint64),
            ColumnValue::Float(val) => impl_prefill!(Float32Builder, val, AnyBuilder::Float32),
            ColumnValue::Double(val) => impl_prefill!(Float64Builder, val, AnyBuilder::Float64),
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
                let mut builder = TimestampMicrosecondBuilder::with_capacity(row_index + 1);
                for _ in 0..row_index {
                    builder.append_null();
                }
                let dt = string_to_datetime(&Utc, val).unwrap();
                builder.append_value(dt.timestamp_micros());
                AnyBuilder::DateTime((builder, None))
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

    pub fn from_field_with_capacity(field: &Field, capacity: usize) -> Self {
        use AnyBuilder::*;

        // Short circuit check for JSON type
        if let Ok(_ext) = field.try_extension_type::<arrow_schema::extension::Json>() {
            return Json(StringBuilder::with_capacity(capacity, 0));
        }

        match field.data_type() {
            DataType::Boolean => Bool(BooleanBuilder::with_capacity(capacity)),
            DataType::Int8 => Int8(Int8Builder::with_capacity(capacity)),
            DataType::UInt8 => Uint8(UInt8Builder::with_capacity(capacity)),
            DataType::Int16 => Int16(Int16Builder::with_capacity(capacity)),
            DataType::UInt16 => Uint16(UInt16Builder::with_capacity(capacity)),
            DataType::Int32 => Int32(Int32Builder::with_capacity(capacity)),
            DataType::UInt32 => Uint32(UInt32Builder::with_capacity(capacity)),
            DataType::Int64 => Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt64 => Uint64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Float64(Float64Builder::with_capacity(capacity)),
            DataType::Utf8 => String(StringBuilder::with_capacity(capacity, 0)),
            DataType::Binary => Binary(BinaryBuilder::with_capacity(capacity, 0)),
            DataType::Date32 => Date32(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(_time_unit, tz) => DateTime((
                TimestampMicrosecondBuilder::with_capacity(capacity),
                tz.clone(),
            )),
            _ => todo!("Unsupported type {}", field.data_type()),
        }
    }

    /// Add a timestamp value
    pub(crate) fn add_timestamp_value(&mut self, value: DateTime<Utc>) -> Result<()> {
        match self {
            AnyBuilder::DateTime((arr, _tz)) => {
                arr.append_value(value.timestamp_micros());
            }
            builder_type => {
                return Err(GeoArrowError::General(format!(
                    "Unexpected type in add_timestamp_value, {:?}",
                    builder_type
                )));
            }
        }
        Ok(())
    }

    /// Add a geozero [ColumnValue]. The type of the value must match the type of the builder.
    pub fn add_value(&mut self, value: &ColumnValue) -> geozero::error::Result<()> {
        use ColumnValue::*;

        macro_rules! impl_add_value {
            ($downcast_func:ident, $v:ident) => {{
                self.$downcast_func().unwrap().append_value(*$v);
            }};
        }

        match value {
            Bool(v) => impl_add_value!(as_bool_mut, v),
            Byte(v) => impl_add_value!(as_int8_mut, v),
            Short(v) => impl_add_value!(as_int16_mut, v),
            Int(v) => impl_add_value!(as_int32_mut, v),
            Long(v) => impl_add_value!(as_int64_mut, v),
            UByte(v) => impl_add_value!(as_uint8_mut, v),
            UShort(v) => impl_add_value!(as_uint16_mut, v),
            UInt(v) => impl_add_value!(as_uint32_mut, v),
            ULong(v) => impl_add_value!(as_uint64_mut, v),
            Float(v) => impl_add_value!(as_float32_mut, v),
            Double(v) => impl_add_value!(as_float64_mut, v),
            String(v) => impl_add_value!(as_string_mut, v),
            Json(v) => impl_add_value!(as_json_mut, v),
            Binary(v) => impl_add_value!(as_binary_mut, v),
            DateTime(v) => {
                let (arr, _tz) = self.as_date_time_mut().unwrap();
                let dt = string_to_datetime(&Utc, v).unwrap();
                arr.append_value(dt.timestamp_micros());
            }
        }

        Ok(())
    }

    /// Append a null value to the builder.
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
            Date32(arr) => arr.append_null(),
            DateTime((arr, _tz)) => arr.append_null(),
            Binary(arr) => arr.append_null(),
        }
    }

    /// Access the [Field] of the builder
    ///
    /// The field is exposed, and not just the data type, to ensure
    ///
    /// There isn't a 1:1 mapping between logical type and an Arrow DataType. For example,
    /// FlatGeobuf and geozero have a "JSON" type, which here gets stored in an Arrow string
    /// column. The relevant `DataType` is `DataType::Utf8`, which then loses information about the
    /// data being JSON. By exporting the `Field`, we can tag that type with an [Arrow JSON
    /// extension type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#json).
    pub fn field(&self) -> Field {
        use AnyBuilder::*;
        match self {
            Bool(_) => Field::new("", DataType::Boolean, true),
            Int8(_) => Field::new("", DataType::Int8, true),
            Uint8(_) => Field::new("", DataType::UInt8, true),
            Int16(_) => Field::new("", DataType::Int16, true),
            Uint16(_) => Field::new("", DataType::UInt16, true),
            Int32(_) => Field::new("", DataType::Int32, true),
            Uint32(_) => Field::new("", DataType::UInt32, true),
            Int64(_) => Field::new("", DataType::Int64, true),
            Uint64(_) => Field::new("", DataType::UInt64, true),
            Float32(_) => Field::new("", DataType::Float32, true),
            Float64(_) => Field::new("", DataType::Float64, true),
            String(_) => Field::new("", DataType::Utf8, true),
            Json(_) => {
                let mut metadata = HashMap::with_capacity(1);
                metadata.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    "arrow.json".to_string(),
                );
                Field::new("", DataType::Utf8, true).with_metadata(metadata)
            }
            Date32(_) => Field::new("", DataType::Date32, true),
            DateTime((_, tz)) => Field::new(
                "",
                DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                true,
            ),
            Binary(_) => Field::new("", DataType::Binary, true),
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
            Date32(arr) => arr.len(),
            DateTime((arr, _tz)) => arr.len(),
            Binary(arr) => arr.len(),
        }
    }

    pub fn finish(self) -> Result<ArrayRef> {
        use AnyBuilder::*;
        let arr: ArrayRef = match self {
            Bool(mut arr) => Arc::new(arr.finish()),
            Int8(mut arr) => Arc::new(arr.finish()),
            Uint8(mut arr) => Arc::new(arr.finish()),
            Int16(mut arr) => Arc::new(arr.finish()),
            Uint16(mut arr) => Arc::new(arr.finish()),
            Int32(mut arr) => Arc::new(arr.finish()),
            Uint32(mut arr) => Arc::new(arr.finish()),
            Int64(mut arr) => Arc::new(arr.finish()),
            Uint64(mut arr) => Arc::new(arr.finish()),
            Float32(mut arr) => Arc::new(arr.finish()),
            Float64(mut arr) => Arc::new(arr.finish()),
            String(mut arr) => Arc::new(arr.finish()),
            Json(mut arr) => Arc::new(arr.finish()),
            Date32(mut arr) => Arc::new(arr.finish()),
            // TODO: how to support timezones? Or is this always naive tz?
            DateTime((mut arr, _tz)) => Arc::new(arr.finish()),
            Binary(mut arr) => Arc::new(arr.finish()),
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
