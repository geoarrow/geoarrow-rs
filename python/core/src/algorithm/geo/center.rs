use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::Center;
use pyo3::prelude::*;

/// Compute the center of geometries
///
/// This first computes the axis-aligned bounding rectangle, then takes the center of
/// that box
///
/// Args:
///     input: input geometry array or chunked geometry array
///
/// Returns:
///     Array or chunked array with center values.
#[pyfunction]
pub fn center(input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = PointArray::from(arr.as_ref().center()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = ChunkedPointArray::from(arr.as_ref().center()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
    }
}
