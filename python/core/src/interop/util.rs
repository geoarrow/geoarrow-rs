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

/// Import pyogrio and assert version 0.8.0 or higher
pub(crate) fn import_pyogrio(py: Python) -> PyGeoArrowResult<Bound<PyModule>> {
    let pyogrio_mod = py.import_bound(intern!(py, "pyogrio"))?;
    let pyogrio_version_string = pyogrio_mod
        .getattr(intern!(py, "__version__"))?
        .extract::<String>()?;
    let pyogrio_major_version = pyogrio_version_string
        .split('.')
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let pyogrio_minor_version = pyogrio_version_string
        .split('.')
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap();
    if pyogrio_major_version < 1 && pyogrio_minor_version < 8 {
        Err(PyValueError::new_err("pyogrio version 0.8 or higher required").into())
    } else {
        Ok(pyogrio_mod)
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
