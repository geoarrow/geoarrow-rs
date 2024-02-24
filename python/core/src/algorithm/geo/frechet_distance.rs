use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{FrechetDistance, FrechetDistanceLineString};
use geoarrow::io::geo::geometry_to_geo;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
pub fn frechet_distance(
    input: AnyGeometryInput,
    other: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, other) {
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Array(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            let result = Float64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Chunked(right)) => {
            let result = FrechetDistance::frechet_distance(&left.as_ref(), &right.as_ref())?;
            let result = ChunkedFloat64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Array(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            let result = Float64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        (AnyGeometryInput::Chunked(left), AnyGeometryBroadcastInput::Scalar(right)) => {
            let scalar = geo::LineString::try_from(geometry_to_geo(&right.0))
                .map_err(|_| PyValueError::new_err("Expected type LineString"))?;
            let result = FrechetDistanceLineString::frechet_distance(&left.as_ref(), &scalar)?;
            let result = ChunkedFloat64Array::from(result);
            Python::with_gil(|py| Ok(result.into_py(py)))
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}
