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

impl_chunks!(ChunkedBooleanArray, BooleanArray);
impl_chunks!(ChunkedFloat16Array, Float16Array);
impl_chunks!(ChunkedFloat32Array, Float32Array);
impl_chunks!(ChunkedFloat64Array, Float64Array);
impl_chunks!(ChunkedUInt8Array, UInt8Array);
impl_chunks!(ChunkedUInt16Array, UInt16Array);
impl_chunks!(ChunkedUInt32Array, UInt32Array);
impl_chunks!(ChunkedUInt64Array, UInt64Array);
impl_chunks!(ChunkedInt8Array, Int8Array);
impl_chunks!(ChunkedInt16Array, Int16Array);
impl_chunks!(ChunkedInt32Array, Int32Array);
impl_chunks!(ChunkedInt64Array, Int64Array);
impl_chunks!(ChunkedStringArray, StringArray);
impl_chunks!(ChunkedLargeStringArray, LargeStringArray);
