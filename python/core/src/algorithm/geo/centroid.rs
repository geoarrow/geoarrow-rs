use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::GeoArrowInput;
use geoarrow::algorithm::geo::Centroid;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Calculation of the centroid.
///
/// The centroid is the arithmetic mean position of all points in the shape.
/// Informally, it is the point at which a cutout of the shape could be perfectly
/// balanced on the tip of a pin.
///
/// The geometric centroid of a convex object always lies in the object.
/// A non-convex object might have a centroid that _is outside the object itself_.
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with centroid values.
#[pyfunction]
pub fn centroid(input: GeoArrowInput) -> PyGeoArrowResult<PointArray> {
    match input {
        GeoArrowInput::Array(arr) => Ok(arr.as_ref().centroid()?.into()),
        _ => Err(PyTypeError::new_err("Expected array").into()),
    }
}

macro_rules! impl_centroid {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculation of the centroid.
            ///
            /// The centroid is the arithmetic mean position of all points in the shape.
            /// Informally, it is the point at which a cutout of the shape could be perfectly
            /// balanced on the tip of a pin.
            ///
            /// The geometric centroid of a convex object always lies in the object.
            /// A non-convex object might have a centroid that _is outside the object itself_.
            ///
            /// Returns:
            ///     Array with centroid values.
            pub fn centroid(&self) -> PointArray {
                use geoarrow::algorithm::geo::Centroid;
                PointArray(Centroid::centroid(&self.0))
            }
        }
    };
}

impl_centroid!(PointArray);
impl_centroid!(LineStringArray);
impl_centroid!(PolygonArray);
impl_centroid!(MultiPointArray);
impl_centroid!(MultiLineStringArray);
impl_centroid!(MultiPolygonArray);
impl_centroid!(MixedGeometryArray);
impl_centroid!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculation of the centroid.
            ///
            /// The centroid is the arithmetic mean position of all points in the shape.
            /// Informally, it is the point at which a cutout of the shape could be perfectly
            /// balanced on the tip of a pin.
            ///
            /// The geometric centroid of a convex object always lies in the object.
            /// A non-convex object might have a centroid that _is outside the object itself_.
            ///
            /// Returns:
            ///     Array with centroid values.
            pub fn centroid(&self) -> PyGeoArrowResult<ChunkedPointArray> {
                use geoarrow::algorithm::geo::Centroid;
                Ok(ChunkedPointArray(Centroid::centroid(&self.0)?))
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
