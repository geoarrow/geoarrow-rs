use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::AffineOps;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub struct AffineTransform(geo::AffineTransform);

impl<'a> FromPyObject<'a> for AffineTransform {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(transform) = ob.extract::<[f64; 6]>() {
            Ok(Self(transform.into()))
        } else if let Ok(transform) = ob.extract::<[f64; 9]>() {
            if transform[6] != 0. || transform[7] != 0. || transform[8] != 1. {
                return Err(PyValueError::new_err(
                    "If 9 values passed, last three must be `0, 0, 1",
                ));
            }

            let transform: [f64; 6] = transform[..6].try_into().unwrap();
            Ok(Self(transform.into()))
        } else {
            Err(PyValueError::new_err("Expected tuple with 6 or 9 elements"))
        }
    }
}

/// Apply an affine transformation to geometries.
///
/// This is intended to be equivalent to [`shapely.affinity.affine_transform`][] for 2D transforms.
///
/// Args:
///     input: input geometry array or chunked geometry array
///     other: an affine transformation to apply to all geometries.
///
///         This integrates with the [`affine`](https://github.com/rasterio/affine) Python
///         library, and most users should use that integration, though it allows any input that
///         is a tuple with 6 or 9 float values.
///
/// Returns:
///     New GeoArrow array or chunked array with the same type as input and with transformed
///     coordinates.
#[pyfunction]
pub fn affine_transform(
    input: AnyGeometryInput,
    transform: AffineTransform,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().affine_transform(&transform.0)?;
            Python::with_gil(|py| geometry_array_to_pyobject(py, out))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().affine_transform(&transform.0)?;
            Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, out))
        }
    }
}
