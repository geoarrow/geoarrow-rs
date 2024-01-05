use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use geoarrow::algorithm::geo::Center;
use geoarrow::array::from_arrow_array;
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
pub fn center(input: &PyAny) -> PyGeoArrowResult<PointArray> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().center()?.into())
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
