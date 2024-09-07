use std::sync::Arc;

use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use crate::util::{return_array, return_chunked_array};
use geoarrow::algorithm::geo::{LineLocatePoint, LineLocatePointScalar};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn line_locate_point(
    py: Python,
    input: AnyGeometryInput,
    point: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, point) {
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Array(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            return_array(py, PyArray::from_array_ref(Arc::new(result)))
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Chunked(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            return_chunked_array(py, PyChunkedArray::from_array_refs(result.chunk_refs())?)
        }
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = point.to_geo_point()?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            return_array(py, PyArray::from_array_ref(Arc::new(result)))
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = point.to_geo_point()?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            return_chunked_array(py, PyChunkedArray::from_array_refs(result.chunk_refs())?)
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
