use crate::array::*;
use crate::chunked_array::*;
use pyo3::prelude::*;

macro_rules! impl_chunks {
    ($chunked_py_array:ty, $py_array:ty) => {
        #[pymethods]
        impl $chunked_py_array {
            /// Number of underlying chunks.
            pub fn num_chunks(&self) -> usize {
                self.0.chunks().len()
            }

            pub fn chunk(&self, i: usize) -> $py_array {
                self.0.chunks()[i].clone().into()
            }

            /// Convert to a list of single-chunked arrays.
            pub fn chunks(&self) -> Vec<$py_array> {
                self.0
                    .chunks()
                    .iter()
                    .map(|chunk| chunk.clone().into())
                    .collect()
            }
        }
    };
}

impl_chunks!(ChunkedPointArray, PointArray);
impl_chunks!(ChunkedLineStringArray, LineStringArray);
impl_chunks!(ChunkedPolygonArray, PolygonArray);
impl_chunks!(ChunkedMultiPointArray, MultiPointArray);
impl_chunks!(ChunkedMultiLineStringArray, MultiLineStringArray);
impl_chunks!(ChunkedMultiPolygonArray, MultiPolygonArray);
impl_chunks!(ChunkedMixedGeometryArray, MixedGeometryArray);
impl_chunks!(ChunkedRectArray, RectArray);
impl_chunks!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
impl_chunks!(ChunkedWKBArray, WKBArray);
