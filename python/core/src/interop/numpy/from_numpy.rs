use crate::array::primitive::*;
use crate::chunked_array::primitive::*;
use crate::error::PyGeoArrowResult;
use arrow_array::PrimitiveArray;
use geoarrow::chunked_array::ChunkedArray;
use numpy::PyReadonlyArray1;
use pyo3::prelude::*;
use pyo3::types::PyType;

macro_rules! impl_array {
    ($struct_name:ty, $rust_scalar:ty) => {
        #[pymethods]
        impl $struct_name {
            /// Construct an array from a Numpy `ndarray`
            #[classmethod]
            pub fn from_numpy(
                _cls: &Bound<PyType>,
                arr: PyReadonlyArray1<$rust_scalar>,
            ) -> PyGeoArrowResult<Self> {
                Ok(Self(PrimitiveArray::from(arr.as_array().to_vec())))
            }
        }
    };
}

impl_array!(Float32Array, f32);
impl_array!(Float64Array, f64);
impl_array!(UInt8Array, u8);
impl_array!(UInt16Array, u16);
impl_array!(UInt32Array, u32);
impl_array!(UInt64Array, u64);
impl_array!(Int8Array, i8);
impl_array!(Int16Array, i16);
impl_array!(Int32Array, i32);
impl_array!(Int64Array, i64);

macro_rules! impl_chunked {
    ($struct_name:ty, $rust_scalar:ty) => {
        #[pymethods]
        impl $struct_name {
            /// Construct a chunked array from a Numpy `ndarray`
            #[classmethod]
            pub fn from_numpy(
                _cls: &Bound<PyType>,
                arr: PyReadonlyArray1<$rust_scalar>,
                lengths: Vec<usize>,
            ) -> PyGeoArrowResult<Self> {
                let arr = PrimitiveArray::from(arr.as_array().to_vec());

                let mut chunks = Vec::with_capacity(lengths.len());
                let mut offset = 0;
                for length in lengths {
                    chunks.push(arr.slice(offset, length));
                    offset += length;
                }

                Ok(Self(ChunkedArray::new(chunks)))
            }
        }
    };
}

impl_chunked!(ChunkedFloat32Array, f32);
impl_chunked!(ChunkedFloat64Array, f64);
impl_chunked!(ChunkedUInt8Array, u8);
impl_chunked!(ChunkedUInt16Array, u16);
impl_chunked!(ChunkedUInt32Array, u32);
impl_chunked!(ChunkedUInt64Array, u64);
impl_chunked!(ChunkedInt8Array, i8);
impl_chunked!(ChunkedInt16Array, i16);
impl_chunked!(ChunkedInt32Array, i32);
impl_chunked!(ChunkedInt64Array, i64);
