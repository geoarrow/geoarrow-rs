use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
use geoarrow::array::from_arrow_array;
use pyo3::prelude::*;

/// Calculate the unsigned approximate geodesic area of a `Geometry`.
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with area values.
#[pyfunction]
pub fn chamberlain_duquette_unsigned_area(input: &PyAny) -> PyGeoArrowResult<Float64Array> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().chamberlain_duquette_unsigned_area()?.into())
}

/// Calculate the signed approximate geodesic area of a `Geometry`.
///
/// Args:
///     input: input geometry array
///
/// Returns:
///     Array with area values.
#[pyfunction]
pub fn chamberlain_duquette_signed_area(input: &PyAny) -> PyGeoArrowResult<Float64Array> {
    let (array, field) = import_arrow_c_array(input)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().chamberlain_duquette_signed_area()?.into())
}

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            ///
            /// Returns:
            ///     Array with area values.
            pub fn chamberlain_duquette_unsigned_area(&self) -> Float64Array {
                ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0).into()
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            ///
            /// Returns:
            ///     Array with area values.
            pub fn chamberlain_duquette_signed_area(&self) -> Float64Array {
                ChamberlainDuquetteArea::chamberlain_duquette_signed_area(&self.0).into()
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
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            ///
            /// Returns:
            ///     Array with area values.
            pub fn chamberlain_duquette_unsigned_area(
                &self,
            ) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0)?.into())
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            ///
            /// Returns:
            ///     Array with area values.
            pub fn chamberlain_duquette_signed_area(
                &self,
            ) -> PyGeoArrowResult<ChunkedFloat64Array> {
                Ok(ChamberlainDuquetteArea::chamberlain_duquette_signed_area(&self.0)?.into())
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
