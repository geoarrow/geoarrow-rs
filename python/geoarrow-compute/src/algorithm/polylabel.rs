use std::sync::Arc;

use crate::ffi::from_python::AnyNativeInput;
use geoarrow::algorithm::polylabel::Polylabel;
use geoarrow::array::NativeArrayDyn;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;
use pyo3_geoarrow::{PyChunkedNativeArray, PyNativeArray};

#[pyfunction]
pub fn polylabel(py: Python, input: AnyNativeInput, tolerance: f64) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(arr) => {
            let out = arr.as_ref().polylabel(tolerance)?;
            Ok(PyNativeArray::new(NativeArrayDyn::new(Arc::new(out)))
                .into_pyobject(py)?
                .into_any()
                .unbind())
        }
        AnyNativeInput::Chunked(chunked) => {
            let out = chunked.as_ref().polylabel(tolerance)?;
            Ok(PyChunkedNativeArray::new(Arc::new(out))
                .into_pyobject(py)?
                .into_any()
                .unbind())
        }
    }
}
