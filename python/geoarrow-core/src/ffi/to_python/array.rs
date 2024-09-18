use std::sync::Arc;

use geoarrow::array::NativeArrayDyn;
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::NativeArray;
use pyo3::prelude::*;
use pyo3_geoarrow::{PyChunkedGeometryArray, PyGeometryArray};

use pyo3_geoarrow::PyGeoArrowResult;

pub fn geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn NativeArray>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyGeometryArray::new(NativeArrayDyn::new(arr)).into_py(py))
}

pub fn chunked_geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn ChunkedGeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyChunkedGeometryArray::new(arr).into_py(py))
}
