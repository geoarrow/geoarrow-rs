use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::Centroid;
use geoarrow::array::GeometryArrayDyn;
use pyo3::prelude::*;

/// Calculation of the centroid.
///
/// The centroid is the arithmetic mean position of all points in the shape.
/// Informally, it is the point at which a cutout of the shape could be perfectly
/// balanced on the tip of a pin.
///
/// The geometric centroid of a convex object always lies in the object.
/// A non-convex object might have a centroid that _is outside the object itself_.
///
/// Args:
///     input: input geometry array or chunked geometry array
///
/// Returns:
///     Array or chunked array with centroid values.
#[pyfunction]
pub fn centroid(py: Python, input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().centroid()?;
            Ok(PyGeometryArray::new(GeometryArrayDyn::new(Arc::new(out))).into_py(py))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().centroid()?;
            Ok(PyChunkedGeometryArray(Arc::new(out)).into_py(py))
        }
    }
}
