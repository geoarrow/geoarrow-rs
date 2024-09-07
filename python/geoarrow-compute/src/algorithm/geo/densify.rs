use crate::ffi::from_python::AnyGeometryInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::Densify;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn densify(
    py: Python,
    input: AnyGeometryInput,
    max_distance: f64,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().densify(max_distance)?;
            return_geometry_array(py, out)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().densify(max_distance)?;
            return_chunked_geometry_array(py, out)
        }
    }
}
