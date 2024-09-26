use crate::ffi::from_python::AnyNativeInput;
use geoarrow::algorithm::native::TotalBounds;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

#[pyfunction]
pub fn total_bounds(input: AnyNativeInput) -> PyGeoArrowResult<(f64, f64, f64, f64)> {
    match input {
        AnyNativeInput::Array(arr) => Ok(arr.as_ref().total_bounds().into()),
        AnyNativeInput::Chunked(arr) => Ok(arr.as_ref().total_bounds().into()),
    }
}
