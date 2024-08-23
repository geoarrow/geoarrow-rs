use crate::chunked_array::*;
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::{to_stream_pycapsule, ArrayIterator};

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
            fn __arrow_c_stream__<'py>(
                &self,
                py: Python<'py>,
                requested_schema: Option<Bound<'py, PyCapsule>>,
            ) -> PyResult<Bound<'py, PyCapsule>> {
                let field = self.0.extension_field();
                let arrow_chunks = self
                    .0
                    .chunks()
                    .iter()
                    .map(|chunk| chunk.to_array_ref())
                    .collect::<Vec<_>>();

                let array_reader =
                    Box::new(ArrayIterator::new(arrow_chunks.into_iter().map(Ok), field));
                to_stream_pycapsule(py, array_reader, requested_schema)
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
