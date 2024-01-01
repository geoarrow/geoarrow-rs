use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use pyo3::prelude::*;

macro_rules! impl_euclidean_length {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// (Euclidean) Calculation of the length of a Line
            pub fn length(&self) -> Float64Array {
                use geoarrow::algorithm::geo::EuclideanLength;
                EuclideanLength::euclidean_length(&self.0).into()
            }
        }
    };
}

impl_euclidean_length!(PointArray);
impl_euclidean_length!(MultiPointArray);
impl_euclidean_length!(LineStringArray);
impl_euclidean_length!(MultiLineStringArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// (Euclidean) Calculation of the length of a Line
            pub fn length(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                use geoarrow::algorithm::geo::EuclideanLength;
                Ok(EuclideanLength::euclidean_length(&self.0)?.into())
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedMultiLineStringArray);
