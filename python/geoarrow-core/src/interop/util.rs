use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

/// Import geopandas and assert version 1.0.0 or higher
pub(crate) fn import_geopandas(py: Python) -> PyGeoArrowResult<Bound<PyModule>> {
    let geopandas_mod = py.import(intern!(py, "geopandas"))?;
    let geopandas_version_string = geopandas_mod
        .getattr(intern!(py, "__version__"))?
        .extract::<String>()?;
    let geopandas_major_version = geopandas_version_string
        .split('.')
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    if geopandas_major_version < 1 {
        Err(PyValueError::new_err("geopandas version 1.0 or higher required").into())
    } else {
        Ok(geopandas_mod)
    }
}

/// Import pyogrio and assert version 0.8.0 or higher
pub(crate) fn import_pyogrio(py: Python) -> PyGeoArrowResult<Bound<PyModule>> {
    let pyogrio_mod = py.import(intern!(py, "pyogrio"))?;
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
