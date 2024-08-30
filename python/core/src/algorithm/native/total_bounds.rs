use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::native::TotalBounds;
use pyo3::prelude::*;

#[pyfunction]
pub fn total_bounds(input: AnyGeometryInput) -> PyGeoArrowResult<(f64, f64, f64, f64)> {
    match input {
        AnyGeometryInput::Array(arr) => Ok(arr.as_ref().total_bounds().into()),
        AnyGeometryInput::Chunked(arr) => Ok(arr.as_ref().total_bounds().into()),
    }
}
