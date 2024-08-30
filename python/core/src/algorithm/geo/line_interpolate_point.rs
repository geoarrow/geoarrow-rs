use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyPrimitiveBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use arrow::datatypes::Float64Type;
use geoarrow::algorithm::geo::LineInterpolatePoint;
use geoarrow::array::GeometryArrayDyn;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
pub fn line_interpolate_point(
    py: Python,
    input: AnyGeometryInput,
    fraction: AnyPrimitiveBroadcastInput<Float64Type>,
) -> PyGeoArrowResult<PyObject> {
    match (input, fraction) {
        (AnyGeometryInput::Array(arr), AnyPrimitiveBroadcastInput::Array(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(&fraction)?;
            Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(out))).into_py(py))
        }
        (AnyGeometryInput::Chunked(arr), AnyPrimitiveBroadcastInput::Chunked(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(fraction.chunks())?;
            Ok(PyChunkedGeometryArray(Arc::new(out)).into_py(py))
        }
        (AnyGeometryInput::Array(arr), AnyPrimitiveBroadcastInput::Scalar(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(fraction)?;
            Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(out))).into_py(py))
        }
        (AnyGeometryInput::Chunked(arr), AnyPrimitiveBroadcastInput::Scalar(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(fraction)?;
            Ok(PyChunkedGeometryArray(Arc::new(out)).into_py(py))
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
