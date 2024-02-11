use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::AnyGeometryInput;
use geoarrow::algorithm::native::TotalBounds;
use pyo3::prelude::*;

/// Computes the total bounds (extent) of the geometry.
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     tuple of (xmin, ymin, xmax, ymax).
#[pyfunction]
pub fn total_bounds(input: AnyGeometryInput) -> PyGeoArrowResult<(f64, f64, f64, f64)> {
    match input {
        AnyGeometryInput::Array(arr) => Ok(arr.as_ref().total_bounds().into()),
        AnyGeometryInput::Chunked(arr) => Ok(arr.as_ref().total_bounds().into()),
    }
}

macro_rules! impl_array {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Computes the total bounds (extent) of the geometry.
            ///
            /// Returns:
            ///     tuple of (xmin, ymin, xmax, ymax).
            pub fn total_bounds(&self) -> (f64, f64, f64, f64) {
                self.0.total_bounds().into()
            }
        }
    };
}

impl_array!(PointArray);
impl_array!(LineStringArray);
impl_array!(PolygonArray);
impl_array!(MultiPointArray);
impl_array!(MultiLineStringArray);
impl_array!(MultiPolygonArray);
impl_array!(MixedGeometryArray);
impl_array!(GeometryCollectionArray);
impl_array!(RectArray);
impl_array!(WKBArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Computes the total bounds (extent) of the geometry.
            ///
            /// Returns:
            ///     tuple of (xmin, ymin, xmax, ymax).
            pub fn total_bounds(&self) -> (f64, f64, f64, f64) {
                self.0.total_bounds().into()
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
impl_chunked!(ChunkedRectArray);
impl_chunked!(ChunkedWKBArray);
