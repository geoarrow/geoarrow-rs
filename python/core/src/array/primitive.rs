use pyo3::prelude::*;

#[pyclass]
pub struct Float64Array(pub(crate) arrow::array::Float64Array);

impl From<arrow::array::Float64Array> for Float64Array {
    fn from(value: arrow::array::Float64Array) -> Self {
        Self(value)
    }
}

impl From<Float64Array> for arrow::array::Float64Array {
    fn from(value: Float64Array) -> Self {
        value.0
    }
}

#[pyclass]
pub struct BooleanArray(pub(crate) arrow::array::BooleanArray);

impl From<arrow::array::BooleanArray> for BooleanArray {
    fn from(value: arrow::array::BooleanArray) -> Self {
        Self(value)
    }
}

impl From<BooleanArray> for arrow::array::BooleanArray {
    fn from(value: BooleanArray) -> Self {
        value.0
    }
}
