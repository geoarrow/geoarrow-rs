use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use geoarrow::array::GeometryArrayDyn;
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::GeometryArrayTrait;

use pyo3::prelude::*;
use std::sync::Arc;

pub fn geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn GeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyGeometryArray(GeometryArrayDyn::new(arr)).into_py(py))
}

pub fn chunked_geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn ChunkedGeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyChunkedGeometryArray(arr).into_py(py))
}
