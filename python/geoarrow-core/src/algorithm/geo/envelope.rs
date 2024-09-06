use std::sync::Arc;

use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::BoundingRect;
use geoarrow::array::GeometryArrayDyn;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;
use pyo3_geoarrow::{PyChunkedGeometryArray, PyGeometryArray};

#[pyfunction]
pub fn envelope(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().bounding_rect()?;
            Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(out))).into_py(py))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().bounding_rect()?;
            Ok(PyChunkedGeometryArray::new(Arc::new(out)).into_py(py))
        }
    }
}
