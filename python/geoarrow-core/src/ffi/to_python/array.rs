use std::sync::Arc;

use geoarrow::array::NativeArrayDyn;
use geoarrow::chunked_array::ChunkedNativeArray;
use geoarrow::NativeArray;
use pyo3::prelude::*;
use pyo3_geoarrow::{PyChunkedNativeArray, PyNativeArray};

use pyo3_geoarrow::PyGeoArrowResult;

pub fn geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn NativeArray>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyNativeArray::new(NativeArrayDyn::new(arr)).into_py(py))
}

pub fn chunked_geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn ChunkedNativeArray>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyChunkedNativeArray::new(arr).into_py(py))
}
