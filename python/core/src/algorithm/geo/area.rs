use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::GeometryInput;
use crate::ffi::GeoArrowInput;
use geoarrow::algorithm::geo::Area;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Unsigned planar area of a geometry array
///
/// Args:
///     input: input geometry array or chunked geometry array
///
/// Returns:
///     Array or chunked array with area values.
#[pyfunction]
pub fn area(input: GeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        GeometryInput::Array(arr) => {
            let out = Float64Array::from(arr.as_ref().unsigned_area()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
        GeometryInput::Chunked(arr) => {
            let out = ChunkedFloat64Array::from(arr.as_ref().unsigned_area()?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
    }
}

/// Signed planar area of a geometry array
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with area values.
#[pyfunction]
pub fn signed_area(input: GeoArrowInput) -> PyGeoArrowResult<Float64Array> {
    match input {
        GeoArrowInput::Array(arr) => Ok(arr.as_ref().signed_area()?.into()),
        _ => Err(PyTypeError::new_err("Expected array").into()),
    }
}

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry array
            ///
            /// Returns:
            ///     Array with area values.
            pub fn area(&self) -> Float64Array {
                Area::unsigned_area(&self.0).into()
            }

            /// Signed planar area of a geometry array
            ///
            /// Returns:
            ///     Array with area values.
            pub fn signed_area(&self) -> Float64Array {
                Area::signed_area(&self.0).into()
            }
        }
    };
}

impl_area!(PointArray);
impl_area!(LineStringArray);
impl_area!(PolygonArray);
impl_area!(MultiPointArray);
impl_area!(MultiLineStringArray);
impl_area!(MultiPolygonArray);
impl_area!(MixedGeometryArray);
impl_area!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry array
            ///
            /// Returns:
            ///     Chunked array with area values.
            pub fn area(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(Area::unsigned_area(&self.0)?.into())
            }

            /// Signed planar area of a geometry array
            ///
            /// Returns:
            ///     Chunked array with area values.
            pub fn signed_area(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(Area::signed_area(&self.0)?.into())
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
