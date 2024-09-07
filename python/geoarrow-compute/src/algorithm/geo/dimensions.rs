use std::sync::Arc;

use crate::ffi::from_python::AnyGeometryInput;
use crate::util::{return_array, return_chunked_array};
use geoarrow::algorithm::geo::HasDimensions;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn is_empty(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = HasDimensions::is_empty(&arr.as_ref())?;
            return_array(py, PyArray::from_array_ref(Arc::new(out)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = HasDimensions::is_empty(&arr.as_ref())?;
            return_chunked_array(py, PyChunkedArray::from_array_refs(out.chunk_refs())?)
        }
    }
}
