use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::ChaikinSmoothing;
use pyo3::prelude::*;

/// Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using Chaikins algorithm.
///
/// [Chaikins smoothing algorithm](http://www.idav.ucdavis.edu/education/CAGDNotes/Chaikins-Algorithm/Chaikins-Algorithm.html)
///
/// Each iteration of the smoothing doubles the number of vertices of the geometry, so in some
/// cases it may make sense to apply a simplification afterwards to remove insignificant
/// coordinates.
///
/// This implementation preserves the start and end vertices of an open linestring and
/// smoothes the corner between start and end of a closed linestring.
///
/// Args:
///     input: input geometry array or chunked geometry array
///     n_iterations: Number of iterations to use for smoothing.
///
/// Returns:
///     Smoothed geometry array or chunked geometry array.
#[pyfunction]
pub fn chaikin_smoothing(input: AnyGeometryInput, n_iterations: u32) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().chaikin_smoothing(n_iterations)?;
            Python::with_gil(|py| geometry_array_to_pyobject(py, out))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().chaikin_smoothing(n_iterations)?;
            Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, out))
        }
    }
}
