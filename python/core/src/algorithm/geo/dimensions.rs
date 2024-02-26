use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::geo::HasDimensions;
use pyo3::prelude::*;

/// Returns True if a geometry is an empty point, polygon, etc.
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Result array.
#[pyfunction]
pub fn is_empty(input: AnyGeometryInput) -> PyGeoArrowResult<PyObject> {
    match input {
        AnyGeometryInput::Array(arr) => {
            let out = BooleanArray::from(HasDimensions::is_empty(&arr.as_ref())?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
        AnyGeometryInput::Chunked(arr) => {
            let out = ChunkedBooleanArray::from(HasDimensions::is_empty(&arr.as_ref())?);
            Python::with_gil(|py| Ok(out.into_py(py)))
        }
    }
}

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Returns True if a geometry is an empty point, polygon, etc.
            ///
            /// Returns:
            ///     Result array.
            pub fn is_empty(&self) -> BooleanArray {
                HasDimensions::is_empty(&self.0).into()
            }
        }
    };
}

impl_alg!(PointArray);
impl_alg!(LineStringArray);
impl_alg!(PolygonArray);
impl_alg!(MultiPointArray);
impl_alg!(MultiLineStringArray);
impl_alg!(MultiPolygonArray);
impl_alg!(MixedGeometryArray);
impl_alg!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Returns True if a geometry is an empty point, polygon, etc.
            ///
            /// Returns:
            ///     Result array.
            pub fn is_empty(&self) -> PyGeoArrowResult<ChunkedBooleanArray> {
                Ok(HasDimensions::is_empty(&self.0)?.into())
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
