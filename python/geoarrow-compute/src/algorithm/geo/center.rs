use std::sync::Arc;

use crate::ffi::from_python::AnyNativeInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::Center;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn center(py: Python, input: AnyNativeInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(arr) => {
            let out = arr.as_ref().center()?;
            return_geometry_array(py, Arc::new(out))
        }
        AnyNativeInput::Chunked(arr) => {
            let out = arr.as_ref().center()?;
            return_chunked_geometry_array(py, Arc::new(out))
        }
    }
}
