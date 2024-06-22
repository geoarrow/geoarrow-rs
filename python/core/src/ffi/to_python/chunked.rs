use crate::chunked_array::*;
use crate::ffi::to_python::ffi_stream::new_stream;
use arrow::datatypes::{Field, FieldRef};
use arrow::error::ArrowError;
use arrow_array::ArrayRef;
use geoarrow::GeometryArrayTrait;

use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CString;
use std::sync::Arc;

/// Trait for types that can read `ArrayRef`'s.
///
/// To create from an iterator, see [ArrayIterator].
pub trait ArrayReader: Iterator<Item = Result<ArrayRef, ArrowError>> {
    /// Returns the field of this `ArrayReader`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn field(&self) -> FieldRef;
}

impl<R: ArrayReader + ?Sized> ArrayReader for Box<R> {
    fn field(&self) -> FieldRef {
        self.as_ref().field()
    }
}

pub struct ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    inner: I::IntoIter,
    inner_field: FieldRef,
}

impl<I> ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    /// Create a new [ArrayIterator].
    ///
    /// If `iter` is an infallible iterator, use `.map(Ok)`.
    pub fn new(iter: I, field: FieldRef) -> Self {
        Self {
            inner: iter.into_iter(),
            inner_field: field,
        }
    }
}

impl<I> Iterator for ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I> ArrayReader for ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    fn field(&self) -> FieldRef {
        self.inner_field.clone()
    }
}

// #[pymethods]
// impl ChunkedPointArray {
//     /// An implementation of the Arrow PyCapsule Interface
//     fn __arrow_c_stream__(&self, _requested_schema: Option<PyObject>) -> PyResult<PyObject> {
//         let field = self.0.extension_field();
//         let arrow_chunks = self
//             .0
//             .chunks()
//             .iter()
//             .map(|chunk| chunk.to_array_ref())
//             .collect::<Vec<_>>();

//         let array_reader = Box::new(ArrayIterator::new(arrow_chunks.into_iter().map(Ok), field));
//         let ffi_stream = new_stream(array_reader);
//         let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

//         Python::with_gil(|py| {
//             let stream_capsule = PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?;
//             Ok(stream_capsule.to_object(py))
//         })
//     }
// }

/// Implement the __arrow_c_stream__ method on a ChunkedGeometryArray
macro_rules! impl_geometry_chunked_array {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the [Arrow PyCapsule
            /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            /// This dunder method should not be called directly, but enables zero-copy
            /// data transfer to other Python libraries that understand Arrow memory.
            ///
            /// For example (as of the upcoming pyarrow v16), you can call
            /// [`pyarrow.chunked_array()`][pyarrow.chunked_array] to convert this array into a
            /// pyarrow array, without copying memory.
            #[allow(unused_variables)]
            fn __arrow_c_stream__(&self, requested_schema: Option<PyObject>) -> PyResult<PyObject> {
                let field = self.0.extension_field();
                let arrow_chunks = self
                    .0
                    .chunks()
                    .iter()
                    .map(|chunk| chunk.to_array_ref())
                    .collect::<Vec<_>>();

                let array_reader =
                    Box::new(ArrayIterator::new(arrow_chunks.into_iter().map(Ok), field));
                let ffi_stream = new_stream(array_reader);
                let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

                Python::with_gil(|py| {
                    let stream_capsule =
                        PyCapsule::new_bound(py, ffi_stream, Some(stream_capsule_name))?;
                    Ok(stream_capsule.to_object(py))
                })
            }
        }
    };
}

impl_geometry_chunked_array!(ChunkedPointArray);
impl_geometry_chunked_array!(ChunkedLineStringArray);
impl_geometry_chunked_array!(ChunkedPolygonArray);
impl_geometry_chunked_array!(ChunkedMultiPointArray);
impl_geometry_chunked_array!(ChunkedMultiLineStringArray);
impl_geometry_chunked_array!(ChunkedMultiPolygonArray);
impl_geometry_chunked_array!(ChunkedMixedGeometryArray);
impl_geometry_chunked_array!(ChunkedGeometryCollectionArray);
impl_geometry_chunked_array!(ChunkedWKBArray);
impl_geometry_chunked_array!(ChunkedRectArray);

/// Implement the __arrow_c_stream__ method on a ChunkedArray
macro_rules! impl_chunked_array {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An implementation of the [Arrow PyCapsule
            /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
            /// This dunder method should not be called directly, but enables zero-copy
            /// data transfer to other Python libraries that understand Arrow memory.
            ///
            /// For example (as of the upcoming pyarrow v16), you can call
            /// [`pyarrow.chunked_array()`][pyarrow.chunked_array] to convert this array into a
            /// pyarrow array, without copying memory.
            #[allow(unused_variables)]
            fn __arrow_c_stream__(&self, requested_schema: Option<PyObject>) -> PyResult<PyObject> {
                let field = Arc::new(Field::new("", self.0.data_type().clone(), true));
                let arrow_chunks = self
                    .0
                    .chunks()
                    .iter()
                    .map(|chunk| Arc::new(chunk.clone()) as ArrayRef)
                    .collect::<Vec<_>>();

                let array_reader =
                    Box::new(ArrayIterator::new(arrow_chunks.into_iter().map(Ok), field));
                let ffi_stream = new_stream(array_reader);
                let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

                Python::with_gil(|py| {
                    let stream_capsule =
                        PyCapsule::new_bound(py, ffi_stream, Some(stream_capsule_name))?;
                    Ok(stream_capsule.to_object(py))
                })
            }
        }
    };
}

impl_chunked_array!(ChunkedBooleanArray);
impl_chunked_array!(ChunkedFloat16Array);
impl_chunked_array!(ChunkedFloat32Array);
impl_chunked_array!(ChunkedFloat64Array);
impl_chunked_array!(ChunkedUInt8Array);
impl_chunked_array!(ChunkedUInt16Array);
impl_chunked_array!(ChunkedUInt32Array);
impl_chunked_array!(ChunkedUInt64Array);
impl_chunked_array!(ChunkedInt8Array);
impl_chunked_array!(ChunkedInt16Array);
impl_chunked_array!(ChunkedInt32Array);
impl_chunked_array!(ChunkedInt64Array);
impl_chunked_array!(ChunkedStringArray);
impl_chunked_array!(ChunkedLargeStringArray);
