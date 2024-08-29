use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::ConvexHull;
use geoarrow::array::{GeometryArrayDyn, PolygonArray};
use geoarrow::chunked_array::ChunkedGeometryArray;
use pyo3::prelude::*;

/// Returns the convex hull of a Polygon. The hull is always oriented
/// counter-clockwise.
///
/// This implementation uses the QuickHull algorithm, based on [Barber, C. Bradford;
/// Dobkin, David P.; Huhdanpaa, Hannu (1 December
/// 1996)](https://dx.doi.org/10.1145%2F235815.235821) Original paper here:
/// <http://www.cs.princeton.edu/~dpd/Papers/BarberDobkinHuhdanpaa.pdf>
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with convex hull polygons.
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
