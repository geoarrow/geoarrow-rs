use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::GeodesicArea;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

/// Determine the perimeter of a geometry on an ellipsoidal model of the earth.
///
/// This uses the geodesic measurement methods given by [Karney (2013)].
///
/// For a polygon this returns the sum of the perimeter of the exterior ring and interior rings.
/// To get the perimeter of just the exterior ring of a polygon, do `polygon.exterior().geodesic_length()`.
///
/// ## Units
///
/// - return value: meter
///
/// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
///
/// Returns:
///     Array with output values.
#[pyfunction]
pub fn geodesic_perimeter(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().geodesic_perimeter()?;
            Ok(PyArray::from_array(out).to_arro3(py)?)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().geodesic_perimeter()?;
            Ok(PyChunkedArray::from_arrays(out.chunks())?.to_arro3(py)?)
        }
    }
}
