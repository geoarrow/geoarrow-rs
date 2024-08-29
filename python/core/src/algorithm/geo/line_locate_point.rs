use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{LineLocatePoint, LineLocatePointScalar};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

/// Returns a fraction of the line's total length
/// representing the location of the closest point on the line to
/// the given point.
///
/// This is intended to be equivalent to [`shapely.line_locate_point`][] when
/// `normalized=True`.
///
/// If the line has zero length the fraction returned is zero.
///
/// If either the point's coordinates or any coordinates of the line
/// are not finite, returns `NaN`.
///
/// Args:
///     input: input geometry array or chunked geometry array
///     point: the fractional distance along the line. A variety of inputs are accepted:
///
///         - A scalar [`Point`][geoarrow.rust.core.Point]
///         - A [`PointArray`][geoarrow.rust.core.PointArray]
///         - A [`ChunkedPointArray`][geoarrow.rust.core.ChunkedPointArray]
///         - Any Python class that implements the Geo Interface, such as a [`shapely` Point][shapely.Point]
///         - Any GeoArrow array or chunked array of `Point` type
///
/// Returns:
///     Array or chunked array with float fraction values.
#[pyfunction]
pub fn line_locate_point(
    py: Python,
    input: AnyGeometryInput,
    point: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, point) {
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Array(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            Ok(PyArray::from_array(result).to_arro3(py)?)
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Chunked(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            Ok(PyChunkedArray::from_arrays(result.chunks())?.to_arro3(py)?)
        }
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = point.to_geo_point()?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            Ok(PyArray::from_array(result).to_arro3(py)?)
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = point.to_geo_point()?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            Ok(PyChunkedArray::from_arrays(result.chunks())?.to_arro3(py)?)
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
