use std::sync::Arc;

use crate::ffi::from_python::AnyNativeInput;
use crate::ffi::from_python::input::AnyPrimitiveBroadcastInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use arrow::datatypes::Float64Type;
use geoarrow::algorithm::geo::LineInterpolatePoint;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn line_interpolate_point(
    py: Python,
    input: AnyNativeInput,
    fraction: AnyPrimitiveBroadcastInput<Float64Type>,
) -> PyGeoArrowResult<PyObject> {
    match (input, fraction) {
        (AnyNativeInput::Array(arr), AnyPrimitiveBroadcastInput::Array(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(&fraction)?;
            return_geometry_array(py, Arc::new(out))
        }
        (AnyNativeInput::Chunked(arr), AnyPrimitiveBroadcastInput::Chunked(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(fraction.chunks())?;
            return_chunked_geometry_array(py, Arc::new(out))
        }
        (AnyNativeInput::Array(arr), AnyPrimitiveBroadcastInput::Scalar(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(fraction)?;
            return_geometry_array(py, Arc::new(out))
        }
        (AnyNativeInput::Chunked(arr), AnyPrimitiveBroadcastInput::Scalar(fraction)) => {
            let out = arr.as_ref().line_interpolate_point(fraction)?;
            return_chunked_geometry_array(py, Arc::new(out))
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
