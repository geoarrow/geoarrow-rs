use crate::ffi::from_python::AnyNativeInput;
use crate::util::{return_chunked_geometry_array, return_geometry_array};
use geoarrow::algorithm::geo::{Simplify, SimplifyVw, SimplifyVwPreserve};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_geoarrow::PyGeoArrowResult;

pub enum SimplifyMethod {
    Rdp,
    Vw,
    VwPreserve,
}

impl<'a> FromPyObject<'a> for SimplifyMethod {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "rdp" => Ok(Self::Rdp),
            "vw" => Ok(Self::Vw),
            "vw_preserve" => Ok(Self::VwPreserve),
            _ => Err(PyValueError::new_err("Unexpected simplify method")),
        }
    }
}

#[pyfunction]
#[pyo3(
    signature = (input, epsilon, *, method = SimplifyMethod::Rdp),
    text_signature = "(input, epsilon, *, method = 'rdp')")
]
pub fn simplify(
    py: Python,
    input: AnyNativeInput,
    epsilon: f64,
    method: SimplifyMethod,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyNativeInput::Array(arr) => {
            let out = match method {
                SimplifyMethod::Rdp => arr.as_ref().simplify(&epsilon)?,
                SimplifyMethod::Vw => arr.as_ref().simplify_vw(&epsilon)?,
                SimplifyMethod::VwPreserve => arr.as_ref().simplify_vw_preserve(&epsilon)?,
            };
            return_geometry_array(py, out)
        }
        AnyNativeInput::Chunked(arr) => {
            let out = match method {
                SimplifyMethod::Rdp => arr.as_ref().simplify(&epsilon)?,
                SimplifyMethod::Vw => arr.as_ref().simplify_vw(&epsilon)?,
                SimplifyMethod::VwPreserve => arr.as_ref().simplify_vw_preserve(&epsilon)?,
            };
            return_chunked_geometry_array(py, out)
        }
    }
}
