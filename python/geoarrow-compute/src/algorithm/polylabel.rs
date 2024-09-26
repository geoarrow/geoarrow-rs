use std::sync::Arc;

use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::polylabel::Polylabel;
use geoarrow::array::NativeArrayDyn;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;
use pyo3_geoarrow::{PyChunkedNativeArray, PyNativeArray};

#[pyfunction]
pub fn polylabel(
    py: Python,
    input: AnyGeometryInput,
    tolerance: f64,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().polylabel(tolerance)?;
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(out))).into_py(py))
        }
        AnyGeometryInput::Chunked(chunked) => {
            let out = chunked.as_ref().polylabel(tolerance)?;
            Ok(PyChunkedNativeArray::new(Arc::new(out)).into_py(py))
        }
    }
}
