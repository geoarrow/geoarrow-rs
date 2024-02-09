use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::geometry_array_to_pyobject;
use crate::ffi::GeoArrowInput;
use geoarrow::algorithm::geo::Simplify;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Simplifies a geometry.
///
/// The [Ramer–Douglas–Peucker
/// algorithm](https://en.wikipedia.org/wiki/Ramer–Douglas–Peucker_algorithm)
/// simplifies a linestring. Polygons are simplified by running the RDP algorithm on
/// all their constituent rings. This may result in invalid Polygons, and has no
/// guarantee of preserving topology.
///
/// Multi* objects are simplified by simplifying all their constituent geometries
/// individually.
///
/// An epsilon less than or equal to zero will return an unaltered version of the
/// geometry.
///
/// Args:
///     input: input geometry array
///     epsilon: tolerance for simplification.
///
/// Returns:
///     Simplified geometry array.
#[pyfunction]
pub fn simplify(input: GeoArrowInput, epsilon: f64) -> PyGeoArrowResult<PyObject> {
    match input {
        GeoArrowInput::Array(arr) => {
            let result = arr.as_ref().simplify(&epsilon)?;
            Python::with_gil(|py| geometry_array_to_pyobject(py, result))
        }
        _ => Err(PyTypeError::new_err("Expected array").into()),
    }
}

macro_rules! impl_simplify {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Simplifies a geometry.
            ///
            /// The [Ramer–Douglas–Peucker
            /// algorithm](https://en.wikipedia.org/wiki/Ramer–Douglas–Peucker_algorithm)
            /// simplifies a linestring. Polygons are simplified by running the RDP algorithm on
            /// all their constituent rings. This may result in invalid Polygons, and has no
            /// guarantee of preserving topology.
            ///
            /// Multi* objects are simplified by simplifying all their constituent geometries
            /// individually.
            ///
            /// An epsilon less than or equal to zero will return an unaltered version of the
            /// geometry.
            ///
            /// Args:
            ///     epsilon: tolerance for simplification.
            ///
            /// Returns:
            ///     Simplified geometry array.
            pub fn simplify(&self, epsilon: f64) -> Self {
                Simplify::simplify(&self.0, &epsilon).into()
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
            /// The [Ramer–Douglas–Peucker
            /// algorithm](https://en.wikipedia.org/wiki/Ramer–Douglas–Peucker_algorithm)
            /// simplifies a linestring. Polygons are simplified by running the RDP algorithm on
            /// all their constituent rings. This may result in invalid Polygons, and has no
            /// guarantee of preserving topology.
            ///
            /// Multi* objects are simplified by simplifying all their constituent geometries
            /// individually.
            ///
            /// An epsilon less than or equal to zero will return an unaltered version of the
            /// geometry.
            ///
            /// Args:
            ///     epsilon: tolerance for simplification.
            ///
            /// Returns:
            ///     Simplified geometry array.
            pub fn simplify(&self, epsilon: f64) -> PyGeoArrowResult<Self> {
                Ok(Simplify::simplify(&self.0, &epsilon).into())
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
