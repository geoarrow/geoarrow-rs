use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::Densify;
use pyo3::prelude::*;

#[pyfunction]
pub fn frechet_distance(
    input: AnyGeometryInput,
    other: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, other) {
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Array(right)) => {}
    }

    // match input {
    //     AnyGeometryInput::Array(arr) => {
    //         let out = arr.as_ref().densify(max_distance)?;
    //         Python::with_gil(|py| geometry_array_to_pyobject(py, out))
    //     }
    //     AnyGeometryInput::Chunked(arr) => {
    //         let out = arr.as_ref().densify(max_distance)?;
    //         Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, out))
    //     }
    // }
}
