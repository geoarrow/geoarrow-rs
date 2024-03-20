use crate::error::PyGeoArrowResult;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;

pub(crate) fn import_shapely(py: Python) -> PyGeoArrowResult<&PyModule> {
    let shapely_mod = py.import(intern!(py, "shapely"))?;
    let shapely_version_string = shapely_mod
        .getattr(intern!(py, "__version__"))?
        .extract::<String>()?;
    if !shapely_version_string.starts_with('2') {
        Err(PyValueError::new_err("Shapely version 2 required").into())
    } else {
        Ok(shapely_mod)
    }
}
