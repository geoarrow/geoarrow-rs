use crate::error::PyGeoArrowResult;
use crate::interop::util::import_geopandas;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

#[pyfunction]
pub fn to_geopandas(py: Python, input: PyObject) -> PyGeoArrowResult<PyObject> {
    let geopandas_mod = import_geopandas(py)?;
    let geodataframe_class = geopandas_mod.getattr(intern!(py, "GeoDataFrame"))?;
    let gdf = geodataframe_class.call_method1(
        intern!(py, "from_arrow"),
        PyTuple::new_bound(py, vec![input]),
    )?;
    Ok(gdf.into())
}
