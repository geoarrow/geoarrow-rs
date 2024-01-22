use pyo3::prelude::*;

macro_rules! impl_primitive_array {
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

impl_primitive_array!(BooleanArray, arrow::array::BooleanArray);
impl_primitive_array!(Float16Array, arrow::array::Float16Array);
impl_primitive_array!(Float32Array, arrow::array::Float32Array);
impl_primitive_array!(Float64Array, arrow::array::Float64Array);
impl_primitive_array!(UInt8Array, arrow::array::UInt8Array);
impl_primitive_array!(UInt16Array, arrow::array::UInt16Array);
impl_primitive_array!(UInt32Array, arrow::array::UInt32Array);
impl_primitive_array!(UInt64Array, arrow::array::UInt64Array);
impl_primitive_array!(Int8Array, arrow::array::Int8Array);
impl_primitive_array!(Int16Array, arrow::array::Int16Array);
impl_primitive_array!(Int32Array, arrow::array::Int32Array);
impl_primitive_array!(Int64Array, arrow::array::Int64Array);
impl_primitive_array!(StringArray, arrow::array::StringArray);
impl_primitive_array!(LargeStringArray, arrow::array::LargeStringArray);
