use geoarrow::chunked_array::ChunkedArray;
use pyo3::prelude::*;

macro_rules! impl_chunked_primitive_array {
    ($struct_name:ident, $arrow_rs_array:ty) => {
        #[pyclass(module = "geoarrow.rust.core._rust")]
        pub struct $struct_name(pub(crate) $arrow_rs_array);

        impl From<$arrow_rs_array> for $struct_name {
            fn from(value: $arrow_rs_array) -> Self {
                Self(value)
            }
        }

        impl From<$struct_name> for $arrow_rs_array {
            fn from(value: $struct_name) -> Self {
                value.0
            }
        }
    };
}

impl_chunked_primitive_array!(ChunkedBooleanArray, ChunkedArray<arrow_array::BooleanArray>);
impl_chunked_primitive_array!(ChunkedFloat16Array, ChunkedArray<arrow_array::Float16Array>);
impl_chunked_primitive_array!(ChunkedFloat32Array, ChunkedArray<arrow_array::Float32Array>);
impl_chunked_primitive_array!(ChunkedFloat64Array, ChunkedArray<arrow_array::Float64Array>);
impl_chunked_primitive_array!(ChunkedUInt8Array, ChunkedArray<arrow_array::UInt8Array>);
impl_chunked_primitive_array!(ChunkedUInt16Array, ChunkedArray<arrow_array::UInt16Array>);
impl_chunked_primitive_array!(ChunkedUInt32Array, ChunkedArray<arrow_array::UInt32Array>);
impl_chunked_primitive_array!(ChunkedUInt64Array, ChunkedArray<arrow_array::UInt64Array>);
impl_chunked_primitive_array!(ChunkedInt8Array, ChunkedArray<arrow_array::Int8Array>);
impl_chunked_primitive_array!(ChunkedInt16Array, ChunkedArray<arrow_array::Int16Array>);
impl_chunked_primitive_array!(ChunkedInt32Array, ChunkedArray<arrow_array::Int32Array>);
impl_chunked_primitive_array!(ChunkedInt64Array, ChunkedArray<arrow_array::Int64Array>);
impl_chunked_primitive_array!(ChunkedStringArray, ChunkedArray<arrow_array::StringArray>);
impl_chunked_primitive_array!(
    ChunkedLargeStringArray,
    ChunkedArray<arrow_array::LargeStringArray>
);
