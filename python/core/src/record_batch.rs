use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct RecordBatch(pub(crate) arrow_array::RecordBatch);

impl From<arrow_array::RecordBatch> for RecordBatch {
    fn from(value: arrow_array::RecordBatch) -> Self {
        Self(value)
    }
}

impl From<RecordBatch> for arrow_array::RecordBatch {
    fn from(value: RecordBatch) -> Self {
        value.0
    }
}
