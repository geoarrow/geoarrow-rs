use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::polylabel::Polylabel;
use geoarrow::array::GeometryArrayDyn;
use pyo3::prelude::*;

/// Calculate a Polygon's ideal label position by calculating its _pole of inaccessibility_.
///
/// The pole of inaccessibility is the most distant internal point from the polygon outline (not to
/// be confused with centroid), and is useful for optimal placement of a text label on a polygon.
///
/// The calculation uses an iterative grid-based algorithm, ported from the original [JavaScript
/// implementation](https://github.com/mapbox/polylabel).
///
/// Args:
///     input: input geometry array or chunked geometry array
///     tolerance: precision of algorithm. Refer to the [original JavaScript
///          documentation](https://github.com/mapbox/polylabel/blob/07c112091b4c9ffeb412af33c575133168893b4a/README.md#how-the-algorithm-works)
///          for more information
///
/// Returns:
///     PointArray or ChunkedPointArray with result values
#[pyfunction]
pub fn polylabel(
    py: Python,
    input: AnyGeometryInput,
    tolerance: f64,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().polylabel(tolerance)?;
            Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(out))).into_py(py))
        }
        AnyGeometryInput::Chunked(chunked) => {
            let out = chunked.as_ref().polylabel(tolerance)?;
            Ok(PyChunkedGeometryArray(Arc::new(out)).into_py(py))
        }
    }
}
