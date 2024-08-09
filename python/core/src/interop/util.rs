use crate::error::PyGeoArrowResult;
use geoarrow::error::GeoArrowError;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;

/// Import pyarrow and assert version 14 or higher.
pub(crate) fn import_pyarrow(py: Python) -> PyGeoArrowResult<Bound<PyModule>> {
    let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
    let pyarrow_version_string = pyarrow_mod
        .getattr(intern!(py, "__version__"))?
        .extract::<String>()?;
    let pyarrow_major_version = pyarrow_version_string
        .split('.')
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    if pyarrow_major_version < 14 {
        Err(PyValueError::new_err("pyarrow version 14.0 or higher required").into())
    } else {
        Ok(pyarrow_mod)
    }
}

pub(crate) fn table_to_pytable(table: geoarrow::table::Table) -> PyTable {
    let (schema, batches) = table.into_inner();
    PyTable::new(batches, schema)
}

pub(crate) fn pytable_to_table(table: PyTable) -> Result<geoarrow::table::Table, GeoArrowError> {
    let (batches, schema) = table.into_inner();
    geoarrow::table::Table::try_new(schema, batches)
}
