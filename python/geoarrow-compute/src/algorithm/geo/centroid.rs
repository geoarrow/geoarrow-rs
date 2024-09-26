use std::sync::Arc;

use crate::ffi::from_python::AnyGeometryInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::Centroid;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn centroid(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().centroid()?;
            return_geometry_array(py, Arc::new(out))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().centroid()?;
            return_chunked_geometry_array(py, Arc::new(out))
        }
    }
}
