use pyo3_geoarrow::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::ChaikinSmoothing;
use pyo3::prelude::*;

#[pyfunction]
pub fn chaikin_smoothing(
    py: Python,
    input: AnyGeometryInput,
    n_iterations: u32,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().chaikin_smoothing(n_iterations)?;
            geometry_array_to_pyobject(py, out)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().chaikin_smoothing(n_iterations)?;
            chunked_geometry_array_to_pyobject(py, out)
        }
    }
}
