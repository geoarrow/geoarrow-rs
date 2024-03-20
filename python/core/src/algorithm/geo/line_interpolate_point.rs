use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyPrimitiveBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use arrow::datatypes::Float64Type;
use geoarrow::algorithm::geo::LineInterpolatePoint;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
pub fn line_interpolate_point(
    input: AnyGeometryInput,
    fraction: AnyPrimitiveBroadcastInput<Float64Type>,
) -> PyGeoArrowResult<PyObject> {
    match (input, fraction) {
        (AnyGeometryInput::Array(arr), AnyPrimitiveBroadcastInput::Array(fraction)) => {
            let result = arr.as_ref().line_interpolate_point(&fraction)?;
            Python::with_gil(|py| Ok(PointArray(result).into_py(py)))
        }
        (AnyGeometryInput::Chunked(arr), AnyPrimitiveBroadcastInput::Chunked(fraction)) => {
            let result = arr.as_ref().line_interpolate_point(fraction.chunks())?;
            Python::with_gil(|py| Ok(ChunkedPointArray(result).into_py(py)))
        }
        (AnyGeometryInput::Array(arr), AnyPrimitiveBroadcastInput::Scalar(fraction)) => {
            let result = arr.as_ref().line_interpolate_point(fraction)?;
            Python::with_gil(|py| Ok(PointArray(result).into_py(py)))
        }
        (AnyGeometryInput::Chunked(arr), AnyPrimitiveBroadcastInput::Scalar(fraction)) => {
            let result = arr.as_ref().line_interpolate_point(fraction)?;
            Python::with_gil(|py| Ok(ChunkedPointArray(result).into_py(py)))
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}

#[pymethods]
impl LineStringArray {
    pub fn line_interpolate_point(
        &self,
        fraction: AnyPrimitiveBroadcastInput<Float64Type>,
    ) -> PyGeoArrowResult<PyObject> {
        line_interpolate_point(AnyGeometryInput::Array(Arc::new(self.0.clone())), fraction)
    }
}

#[pymethods]
impl ChunkedLineStringArray {
    pub fn line_interpolate_point(
        &self,
        fraction: AnyPrimitiveBroadcastInput<Float64Type>,
    ) -> PyGeoArrowResult<PyObject> {
        line_interpolate_point(
            AnyGeometryInput::Chunked(Arc::new(self.0.clone())),
            fraction,
        )
    }
}
