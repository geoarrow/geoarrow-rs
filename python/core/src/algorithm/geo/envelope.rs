use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::BoundingRect;
use geoarrow::array::GeometryArrayDyn;
use pyo3::prelude::*;

/// Computes the minimum axis-aligned bounding box that encloses an input geometry
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with axis-aligned bounding boxes.
#[pyfunction]
pub fn envelope(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().bounding_rect()?;
            Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(out))).into_py(py))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().bounding_rect()?;
            Ok(PyChunkedGeometryArray(Arc::new(out)).into_py(py))
        }
    }
}
