use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::GeodesicArea;
use pyo3::prelude::*;
use pyo3_arrow::{PyArray, PyChunkedArray};

#[pyfunction]
pub fn geodesic_perimeter(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().geodesic_perimeter()?;
            Ok(PyArray::from_array(out).to_arro3(py)?)
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().geodesic_perimeter()?;
            Ok(PyChunkedArray::from_arrays(out.chunks())?.to_arro3(py)?)
        }
    }
}
