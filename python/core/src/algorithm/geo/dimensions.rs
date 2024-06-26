use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::HasDimensions;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

/// Returns True if a geometry is an empty point, polygon, etc.
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Result array.
#[pyfunction]
pub fn is_empty(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = HasDimensions::is_empty(&arr.as_ref())?;
            Ok(PyArray::from_array(out).to_arro3(py)?)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = HasDimensions::is_empty(&arr.as_ref())?;
            Ok(PyChunkedArray::from_arrays(out.chunks())?.to_arro3(py)?)
        }
    }
}
