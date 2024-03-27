use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct Schema(pub(crate) arrow::datatypes::SchemaRef);

impl From<arrow::datatypes::SchemaRef> for Schema {
    fn from(value: arrow::datatypes::SchemaRef) -> Self {
        Self(value)
    }
}

impl From<Schema> for arrow::datatypes::SchemaRef {
    fn from(value: Schema) -> Self {
        value.0
    }
}
