use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::ConvexHull;
use geoarrow::array::{GeometryArrayDyn, PolygonArray};
use geoarrow::chunked_array::ChunkedGeometryArray;
use pyo3::prelude::*;

#[pyfunction]
pub fn convex_hull(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out: PolygonArray<i32, 2> = arr.as_ref().convex_hull()?;
            Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(out))).into_py(py))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out: ChunkedGeometryArray<PolygonArray<i32, 2>> = arr.as_ref().convex_hull()?;
            Ok(PyChunkedGeometryArray(Arc::new(out)).into_py(py))
        }
    }
}
