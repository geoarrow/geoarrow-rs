use crate::array::*;
use crate::chunked_array::*;
use geoarrow::algorithm::native::Concatenate;
use pyo3::prelude::*;

macro_rules! impl_len {
    ($struct_name:ident, $return_type:ty) => {
        #[pymethods]
        impl $struct_name {
            /// Concatenate a chunked array into a contiguous array.
            pub fn concatenate(&self) -> $return_type {
                self.0.concatenate().unwrap().into()
            }
        }
    };
}

impl_len!(ChunkedPointArray, PointArray);
impl_len!(ChunkedLineStringArray, LineStringArray);
impl_len!(ChunkedPolygonArray, PolygonArray);
impl_len!(ChunkedMultiPointArray, MultiPointArray);
impl_len!(ChunkedMultiLineStringArray, MultiLineStringArray);
impl_len!(ChunkedMultiPolygonArray, MultiPolygonArray);
impl_len!(ChunkedMixedGeometryArray, MixedGeometryArray);
impl_len!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
