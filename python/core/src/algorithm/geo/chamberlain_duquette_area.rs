use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use pyo3::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            pub fn chamberlain_duquette_unsigned_area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0).into()
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            pub fn chamberlain_duquette_signed_area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
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
            pub fn chamberlain_duquette_unsigned_area(
                &self,
            ) -> PyGeoArrowResult<ChunkedFloat64Array> {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                Ok(ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0)?.into())
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            pub fn chamberlain_duquette_signed_area(
                &self,
            ) -> PyGeoArrowResult<ChunkedFloat64Array> {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
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
