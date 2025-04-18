use crate::interop::util::import_geopandas;
use pyo3::PyAny;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3_arrow::PyTable;
use pyo3_arrow::export::Arro3Table;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn from_geopandas(py: Python, input: &Bound<PyAny>) -> PyGeoArrowResult<Arro3Table> {
    let geopandas_mod = import_geopandas(py)?;
    let geodataframe_class = geopandas_mod.getattr(intern!(py, "GeoDataFrame"))?;
    if !input.is_instance(&geodataframe_class)? {
        return Err(PyValueError::new_err("Expected GeoDataFrame input.").into());
    }

    // Note: I got an error in test_write_native_multi_points in `from_geopandas` with the WKB
    // encoding
    let kwargs = PyDict::new(py);
    kwargs.set_item("geometry_encoding", "geoarrow")?;
    let table = input
        .call_method(
            intern!(py, "to_arrow"),
            PyTuple::new(py, std::iter::empty::<PyObject>())?,
            Some(&kwargs),
        )?
        .extract::<PyTable>()?;

    Ok(table.into())
}
