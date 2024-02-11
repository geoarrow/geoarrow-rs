use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use crate::ffi::to_python::{chunked_geometry_array_to_pyobject, geometry_array_to_pyobject};
use geoarrow::algorithm::geo::Densify;
use pyo3::prelude::*;

/// Return a new linear geometry containing both existing and new interpolated
/// coordinates with a maximum distance of `max_distance` between them.
///
/// Note: `max_distance` must be greater than 0.
///
/// Args:
///     input: input geometry array
///     max_distance: maximum distance between coordinates
///
/// Returns:
///     Densified geometry array
#[pyfunction]
pub fn densify(input: AnyGeometryInput, max_distance: f64) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = arr.as_ref().densify(max_distance)?;
            Python::with_gil(|py| geometry_array_to_pyobject(py, out))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = arr.as_ref().densify(max_distance)?;
            Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, out))
        }
    }
}

macro_rules! impl_densify {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Return a new linear geometry containing both existing and new interpolated
            /// coordinates with a maximum distance of `max_distance` between them.
            ///
            /// Note: `max_distance` must be greater than 0.
            ///
            /// Args:
            ///     max_distance: maximum distance between coordinates
            ///
            /// Returns:
            ///     Densified geometry array
            pub fn densify(&self, max_distance: f64) -> Self {
                Densify::densify(&self.0, max_distance).into()
            }
        }
    };
}

impl_densify!(LineStringArray);
impl_densify!(PolygonArray);
impl_densify!(MultiLineStringArray);
impl_densify!(MultiPolygonArray);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Return a new linear geometry containing both existing and new interpolated
            /// coordinates with a maximum distance of `max_distance` between them.
            ///
            /// Note: `max_distance` must be greater than 0.
            ///
            /// Args:
            ///     max_distance: maximum distance between coordinates
            ///
            /// Returns:
            ///     Densified geometry array
            pub fn densify(&self, max_distance: f64) -> Self {
                Densify::densify(&self.0, max_distance).into()
            }
        }
    };
}

impl_vector!(ChunkedLineStringArray);
impl_vector!(ChunkedPolygonArray);
impl_vector!(ChunkedMultiLineStringArray);
impl_vector!(ChunkedMultiPolygonArray);
