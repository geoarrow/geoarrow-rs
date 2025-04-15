use std::sync::Arc;

use crate::ffi::from_python::AnyNativeInput;
use crate::ffi::from_python::input::AnyNativeBroadcastInput;
use crate::util::{return_array, return_chunked_array};
use geoarrow::algorithm::geo::{FrechetDistance, FrechetDistanceLineString};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn frechet_distance(
    py: Python,
    input: AnyNativeInput,
    other: AnyNativeBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, other) {
        (AnyNativeInput::Array(left), AnyNativeBroadcastInput::Array(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            return_array(py, PyArray::from_array_ref(Arc::new(result)))
        }
        (AnyNativeInput::Chunked(left), AnyNativeBroadcastInput::Chunked(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            return_chunked_array(py, PyChunkedArray::from_array_refs(result.chunk_refs())?)
        }
        (AnyNativeInput::Array(left), AnyNativeBroadcastInput::Scalar(right)) => {
            let scalar = right.to_geo_line_string()?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            return_array(py, PyArray::from_array_ref(Arc::new(result)))
        }
        (AnyNativeInput::Chunked(left), AnyNativeBroadcastInput::Scalar(right)) => {
            let scalar = right.to_geo_line_string()?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            return_chunked_array(py, PyChunkedArray::from_array_refs(result.chunk_refs())?)
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
