use std::sync::Arc;

use geoarrow::array::GeometryArrayDyn;
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;
use pyo3_geoarrow::{PyChunkedGeometryArray, PyGeometryArray};

use crate::error::PyGeoArrowResult;

pub fn geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn GeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyGeometryArray::new(GeometryArrayDyn::new(arr)).into_py(py))
}

pub fn chunked_geometry_array_to_pyobject(
    py: Python,
    arr: Arc<dyn ChunkedGeometryArrayTrait>,
) -> PyGeoArrowResult<PyObject> {
    Ok(PyChunkedGeometryArray::new(arr).into_py(py))
}
