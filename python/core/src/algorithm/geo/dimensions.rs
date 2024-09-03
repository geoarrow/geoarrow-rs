use std::sync::Arc;

use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::HasDimensions;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

#[pyfunction]
pub fn is_empty(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = HasDimensions::is_empty(&arr.as_ref())?;
            Ok(PyArray::from_array_ref(Arc::new(out)).to_arro3(py)?)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = HasDimensions::is_empty(&arr.as_ref())?;
            Ok(PyChunkedArray::from_array_refs(out.chunk_refs())?.to_arro3(py)?)
        }
    }
}
