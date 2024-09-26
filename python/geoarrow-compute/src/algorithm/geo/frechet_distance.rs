use std::sync::Arc;

use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use crate::util::{return_array, return_chunked_array};
use geoarrow::algorithm::geo::{FrechetDistance, FrechetDistanceLineString};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn frechet_distance(
    py: Python,
    input: AnyGeometryInput,
    other: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, other) {
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Array(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            return_array(py, PyArray::from_array_ref(Arc::new(result)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Chunked(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            return_chunked_array(py, PyChunkedArray::from_array_refs(result.chunk_refs())?)
        }
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = right.to_geo_line_string()?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            return_array(py, PyArray::from_array_ref(Arc::new(result)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = right.to_geo_line_string()?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            return_chunked_array(py, PyChunkedArray::from_array_refs(result.chunk_refs())?)
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
