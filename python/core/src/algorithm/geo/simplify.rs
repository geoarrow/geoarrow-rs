use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::{Simplify, SimplifyVw, SimplifyVwPreserve};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

pub enum SimplifyMethod {
    Rdp,
    Vw,
    VwPreserve,
}

impl<'a> FromPyObject<'a> for SimplifyMethod {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "rdp" => Ok(Self::Rdp),
            "vw" => Ok(Self::Vw),
            "vw_preserve" => Ok(Self::VwPreserve),
            _ => Err(PyValueError::new_err("Unexpected simplify method")),
        }
    }
}

/// Simplifies a geometry.
///
/// Args:
///     input: input geometry array
///     epsilon: tolerance for simplification. An epsilon less than or equal to zero will return an
///         unaltered version of the geometry.
///
/// Other args:
///      method: The method to use for simplification calculation. One of "rdp" or "vw". Refer to
///         the documentation on [SimplifyMethod][geoarrow.rust.core.enums.SimplifyMethod] for more
///         information.
///
/// Returns:
///     Simplified geometry array.
#[pyfunction]
#[pyo3(
    signature = (input, epsilon, *, method = SimplifyMethod::Rdp),
    text_signature = "(input, epsilon, *, method = 'rdp')")
]
pub fn simplify(
    input: AnyGeometryInput,
    epsilon: f64,
    method: SimplifyMethod,
) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = match method {
                SimplifyMethod::Rdp => arr.as_ref().simplify(&epsilon)?,
                SimplifyMethod::Vw => arr.as_ref().simplify_vw(&epsilon)?,
                SimplifyMethod::VwPreserve => arr.as_ref().simplify_vw_preserve(&epsilon)?,
            };
            Python::with_gil(|py| geometry_array_to_pyobject(py, out))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = match method {
                SimplifyMethod::Rdp => arr.as_ref().simplify(&epsilon)?,
                SimplifyMethod::Vw => arr.as_ref().simplify_vw(&epsilon)?,
                SimplifyMethod::VwPreserve => arr.as_ref().simplify_vw_preserve(&epsilon)?,
            };
            Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, out))
        }
    }
}

macro_rules! impl_simplify {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Simplifies a geometry.
            ///
            /// Args:
            ///     epsilon: tolerance for simplification. An epsilon less than or equal to zero
            ///         will return an unaltered version of the geometry.
            ///
            /// Other args:
            ///      method: The method to use for simplification calculation. One of "rdp" or
            ///         "vw". Refer to the documentation on
            ///         [SimplifyMethod][geoarrow.rust.core.enums.SimplifyMethod] for more
            ///         information.
            ///
            /// Returns:
            ///     Simplified geometry array.
            #[pyo3(signature = (epsilon, *, method = SimplifyMethod::Rdp), text_signature = "(epsilon, *, method = 'rdp')")]
            pub fn simplify(&self, epsilon: f64, method: SimplifyMethod) -> Self {
                match method {
                    SimplifyMethod::Rdp => self.0.simplify(&epsilon).into(),
                    SimplifyMethod::Vw => self.0.simplify_vw(&epsilon).into(),
                    SimplifyMethod::VwPreserve => self.0.simplify_vw_preserve(&epsilon).into(),
                }
            }
        }
    };
}

impl_simplify!(PointArray);
impl_simplify!(LineStringArray);
impl_simplify!(PolygonArray);
impl_simplify!(MultiPointArray);
impl_simplify!(MultiLineStringArray);
impl_simplify!(MultiPolygonArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Simplifies a geometry.
            ///
            /// Args:
            ///     epsilon: tolerance for simplification. An epsilon less than or equal to zero
            ///         will return an unaltered version of the geometry.
            ///
            /// Other args:
            ///      method: The method to use for simplification calculation. One of "rdp" or
            ///         "vw". Refer to the documentation on
            ///         [SimplifyMethod][geoarrow.rust.core.enums.SimplifyMethod] for more
            ///         information.
            ///
            /// Returns:
            ///     Simplified geometry array.
            #[pyo3(signature = (epsilon, *, method = SimplifyMethod::Rdp), text_signature = "(epsilon, *, method = 'rdp')")]
            pub fn simplify(&self, epsilon: f64, method: SimplifyMethod) -> Self {
                match method {
                    SimplifyMethod::Rdp => self.0.simplify(&epsilon).into(),
                    SimplifyMethod::Vw => self.0.simplify_vw(&epsilon).into(),
                    SimplifyMethod::VwPreserve => self.0.simplify_vw_preserve(&epsilon).into(),
                }
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
