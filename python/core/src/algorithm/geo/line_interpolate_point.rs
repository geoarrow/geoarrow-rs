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

/// Returns a point interpolated at given distance on a line.
///
/// This is intended to be equivalent to [`shapely.line_interpolate_point`][] when
/// `normalized=True`.
///
/// If the given fraction is
///  * less than zero (including negative infinity): returns the starting point
///  * greater than one (including infinity): returns the ending point
///  * If either the fraction is NaN, or any coordinates of the line are not
///    finite, returns `Point EMPTY`.
///
/// Args:
///     input: input geometry array or chunked geometry array
///     fraction: the fractional distance along the line. A variety of inputs are accepted:
///
///         - A Python `float` or `int`
///         - A numpy `ndarray` with `float64` data type.
///         - An Arrow array or chunked array with `float64` data type.
///
/// Returns:
///     PointArray or ChunkedPointArray with result values
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
