use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::Densify;
use pyo3::prelude::*;

/// Return a new linear geometry containing both existing and new interpolated
/// coordinates with a maximum distance of `max_distance` between them.
///
/// Note: `max_distance` must be greater than 0.
///
/// Args:
///     input: input geometry array
///     max_distance: maximum distance between coordinates
///
/// Returns:
///     Densified geometry array
#[pyfunction]
pub fn densify(
    py: Python,
    input: AnyGeometryInput,
    max_distance: f64,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().densify(max_distance)?;
            geometry_array_to_pyobject(py, out)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().densify(max_distance)?;
            chunked_geometry_array_to_pyobject(py, out)
        }
    }
}
