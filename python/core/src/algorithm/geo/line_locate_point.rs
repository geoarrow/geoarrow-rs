use std::sync::Arc;

use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{LineLocatePoint, LineLocatePointScalar};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

#[pyfunction]
pub fn line_locate_point(
    py: Python,
    input: AnyGeometryInput,
    point: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, point) {
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Array(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            Ok(PyArray::from_array_ref(Arc::new(result)).to_arro3(py)?)
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Chunked(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            Ok(PyChunkedArray::from_array_refs(result.chunk_refs())?.to_arro3(py)?)
        }
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = point.to_geo_point()?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            Ok(PyArray::from_array_ref(Arc::new(result)).to_arro3(py)?)
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = point.to_geo_point()?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            Ok(PyChunkedArray::from_array_refs(result.chunk_refs())?.to_arro3(py)?)
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
