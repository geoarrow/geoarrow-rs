use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::GeometryInput;
use crate::ffi::GeoArrowInput;
use geoarrow::algorithm::geo::Center;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Compute the center of geometries
///
/// This first computes the axis-aligned bounding rectangle, then takes the center of
/// that box
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with center values.
#[pyfunction]
pub fn center(input: GeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        GeometryInput::Array(arr) => {
            let out = PointArray::from(arr.as_ref().center()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
        GeometryInput::Chunked(arr) => {
            let out = ChunkedPointArray::from(arr.as_ref().center()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
    }
}

macro_rules! impl_center {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Compute the center of geometries
            ///
            /// This first computes the axis-aligned bounding rectangle, then takes the center of
            /// that box
            ///
            /// Returns:
            ///     Array with center values.
            pub fn center(&self) -> PointArray {
                use geoarrow::algorithm::geo::Center;
                PointArray(Center::center(&self.0))
            }
        }
    };
}

impl_center!(PointArray);
impl_center!(LineStringArray);
impl_center!(PolygonArray);
impl_center!(MultiPointArray);
impl_center!(MultiLineStringArray);
impl_center!(MultiPolygonArray);
impl_center!(MixedGeometryArray);
impl_center!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Compute the center of geometries
            ///
            /// This first computes the axis-aligned bounding rectangle, then takes the center of
            /// that box
            ///
            /// Returns:
            ///     Array with center values.
            pub fn center(&self) -> PyGeoArrowResult<ChunkedPointArray> {
                use geoarrow::algorithm::geo::Center;
                Ok(ChunkedPointArray(Center::center(&self.0)?))
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
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);
