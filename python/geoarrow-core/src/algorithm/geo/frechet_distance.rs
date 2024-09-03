use std::sync::Arc;

use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{FrechetDistance, FrechetDistanceLineString};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

#[pyfunction]
pub fn frechet_distance(
    py: Python,
    input: AnyGeometryInput,
    other: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, other) {
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Array(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            Ok(PyArray::from_array_ref(Arc::new(result)).to_arro3(py)?)
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Chunked(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            Ok(PyChunkedArray::from_array_refs(result.chunk_refs())?.to_arro3(py)?)
        }
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = right.to_geo_line_string()?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            Ok(PyArray::from_array_ref(Arc::new(result)).to_arro3(py)?)
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = right.to_geo_line_string()?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            Ok(PyChunkedArray::from_array_refs(result.chunk_refs())?.to_arro3(py)?)
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
