use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::input::AnyGeometryBroadcastInput;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::{LineLocatePoint, LineLocatePointScalar};
use geoarrow::io::geo::geometry_to_geo;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
pub fn line_locate_point(
    input: AnyGeometryInput,
    point: AnyGeometryBroadcastInput,
) -> PyGeoArrowResult<PyObject> {
    match (input, point) {
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Array(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            Python::with_gil(|py| Ok(Float64Array(result).into_py(py)))
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Chunked(point)) => {
            let result = LineLocatePoint::line_locate_point(&arr.as_ref(), point.as_ref())?;
            Python::with_gil(|py| Ok(ChunkedFloat64Array(result).into_py(py)))
        }
        (AnyGeometryInput::Array(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = geo::Point::try_from(geometry_to_geo(&point.0))
                .map_err(|_| PyValueError::new_err("Expected type Point"))?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            Python::with_gil(|py| Ok(Float64Array(result).into_py(py)))
        }
        (AnyGeometryInput::Chunked(arr), AnyGeometryBroadcastInput::Scalar(point)) => {
            let scalar = geo::Point::try_from(geometry_to_geo(&point.0))
                .map_err(|_| PyValueError::new_err("Expected type Point"))?;
            let result = LineLocatePointScalar::line_locate_point(&arr.as_ref(), &scalar)?;
            Python::with_gil(|py| Ok(ChunkedFloat64Array(result).into_py(py)))
        }
        _ => Err(PyValueError::new_err("Unsupported input types.").into()),
    }
}

#[pymethods]
impl LineStringArray {
    pub fn line_locate_point(
        &self,
        point: AnyGeometryBroadcastInput,
    ) -> PyGeoArrowResult<PyObject> {
        let input = AnyGeometryInput::Array(Arc::new(self.0.clone()));
        line_locate_point(input, point)
    }
}

#[pymethods]
impl ChunkedLineStringArray {
    pub fn line_locate_point(
        &self,
        point: AnyGeometryBroadcastInput,
    ) -> PyGeoArrowResult<PyObject> {
        let input = AnyGeometryInput::Chunked(Arc::new(self.0.clone()));
        line_locate_point(input, point)
    }
}
