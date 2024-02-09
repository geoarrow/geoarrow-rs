use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::GeoArrowInput;
use geoarrow::algorithm::geo::BoundingRect;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Computes the minimum axis-aligned bounding box that encloses an input geometry
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with axis-aligned bounding boxes.
#[pyfunction]
pub fn envelope(input: GeoArrowInput) -> PyGeoArrowResult<RectArray> {
    match input {
        GeoArrowInput::Array(arr) => Ok(arr.as_ref().bounding_rect()?.into()),
        _ => Err(PyTypeError::new_err("Expected array").into()),
    }
}

macro_rules! impl_envelope {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Computes the minimum axis-aligned bounding box that encloses an input geometry
            ///
            /// Returns:
            ///     Array with axis-aligned bounding boxes.
            pub fn envelope(&self) -> RectArray {
                RectArray(BoundingRect::bounding_rect(&self.0))
            }
        }
    };
}

impl_envelope!(PointArray);
impl_envelope!(LineStringArray);
impl_envelope!(PolygonArray);
impl_envelope!(MultiPointArray);
impl_envelope!(MultiLineStringArray);
impl_envelope!(MultiPolygonArray);
impl_envelope!(MixedGeometryArray);
impl_envelope!(GeometryCollectionArray);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Computes the minimum axis-aligned bounding box that encloses an input geometry
            ///
            /// Returns:
            ///     Array with axis-aligned bounding boxes.
            pub fn envelope(&self) -> PyGeoArrowResult<ChunkedRectArray> {
                Ok(ChunkedRectArray(BoundingRect::bounding_rect(&self.0)?))
            }
        }
    };
}

impl_vector!(ChunkedPointArray);
impl_vector!(ChunkedLineStringArray);
impl_vector!(ChunkedPolygonArray);
impl_vector!(ChunkedMultiPointArray);
impl_vector!(ChunkedMultiLineStringArray);
impl_vector!(ChunkedMultiPolygonArray);
impl_vector!(ChunkedMixedGeometryArray);
impl_vector!(ChunkedGeometryCollectionArray);
