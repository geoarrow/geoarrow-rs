use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::polylabel::Polylabel;
use pyo3::prelude::*;

#[pyfunction]
pub fn polylabel(input: AnyGeometryInput, tolerance: f64) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let result = arr.as_ref().polylabel(tolerance)?;
            Python::with_gil(|py| Ok(PointArray(result).into_py(py)))
        }
        AnyGeometryInput::Chunked(chunked) => {
            let result = chunked.as_ref().polylabel(tolerance)?;
            Python::with_gil(|py| Ok(ChunkedPointArray(result).into_py(py)))
        }
    }
}

#[pymethods]
impl PolygonArray {
    pub fn polylabel(&self, tolerance: f64) -> PyGeoArrowResult<PyObject> {
        polylabel(AnyGeometryInput::Array(Arc::new(self.0.clone())), tolerance)
    }
}

#[pymethods]
impl ChunkedPolygonArray {
    pub fn polylabel(&self, tolerance: f64) -> PyGeoArrowResult<PyObject> {
        polylabel(
            AnyGeometryInput::Chunked(Arc::new(self.0.clone())),
            tolerance,
        )
    }
}
