use crate::error::PyGeoArrowResult;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;

/// Import pyarrow and assert version 14 or higher.
pub(crate) fn import_pyarrow(py: Python) -> PyGeoArrowResult<&PyModule> {
    let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
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
