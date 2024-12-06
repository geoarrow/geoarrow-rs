use pyo3::prelude::*;
use pyo3_arrow::PyTable;

/// A wrapper around a [PyTable] that implements [IntoPyObject] to convert to a runtime-available
/// arro3.core.Table
///
/// This ensures that we return with the user's runtime-provided arro3.core.Table and not the one
/// we linked from Rust.
pub struct Arro3Table(PyTable);

impl Arro3Table {
    pub fn from_geoarrow(table: geoarrow::table::Table) -> Self {
        let (batches, schema) = table.into_inner();
        PyTable::try_new(batches, schema).unwrap().into()
    }
}

impl From<PyTable> for Arro3Table {
    fn from(value: PyTable) -> Self {
        Self(value)
    }
}

impl<'py> IntoPyObject<'py> for Arro3Table {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.0.to_arro3(py)?.bind(py).clone())
    }
}
