use crate::ffi::from_python::AnyNativeInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::AffineOps;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

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

#[pyfunction]
pub fn affine_transform(
    py: Python,
    input: AnyNativeInput,
    transform: AffineTransform,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(arr) => {
            let out = arr.as_ref().affine_transform(&transform.0)?;
            return_geometry_array(py, out)
        }
        AnyNativeInput::Chunked(arr) => {
            let out = arr.as_ref().affine_transform(&transform.0)?;
            return_chunked_geometry_array(py, out)
        }
    }
}
