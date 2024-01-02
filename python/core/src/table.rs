use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::chunked_geometry_array_to_pyobject;
use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct GeoTable(pub(crate) geoarrow::table::GeoTable);

#[pymethods]
impl GeoTable {
    /// Access the geometry column of this table
    pub fn geometry(&self) -> PyGeoArrowResult<PyObject> {
        let chunked_geom_arr = self.0.geometry()?;
        Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, chunked_geom_arr))
    }
}
