use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::error::GeoArrowError;
use geozero::ProcessToJson;
use pyo3::intern;
use pyo3::prelude::*;

#[pymethods]
impl GeoTable {
    /// Implements the "geo interface protocol".
    ///
    /// See <https://gist.github.com/sgillies/2217756>
    #[getter]
    pub fn __geo_interface__<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
        let mut table = self.0.clone();
        let json_string = table.to_json().map_err(GeoArrowError::GeozeroError)?;
        let json_mod = py.import(intern!(py, "json"))?;
        let args = (json_string.into_py(py),);
        Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
    }
}
