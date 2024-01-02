use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use crate::ffi::to_python::geometry_array_to_pyobject;
use geoarrow::algorithm::geo::Densify;
use geoarrow::array::from_arrow_array;
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
pub fn densify(input: &PyAny, max_distance: f64) -> PyGeoArrowResult<PyObject> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    let result = array.as_ref().densify(max_distance)?;
    Python::with_gil(|py| geometry_array_to_pyobject(py, result))
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
