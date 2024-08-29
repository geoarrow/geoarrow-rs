use std::sync::Arc;

use geoarrow::array::from_arrow_array;
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::{to_stream_pycapsule, ArrayIterator};

use crate::array::PyGeometryArray;
use crate::error::PyGeoArrowResult;
use crate::scalar::PyGeometry;

/// An immutable chunked array of geometries using GeoArrow's in-memory representation.
#[pyclass(
    module = "geoarrow.rust.core._rust",
    name = "ChunkedGeometryArray",
    subclass
)]
pub struct PyChunkedGeometryArray(pub(crate) Arc<dyn ChunkedGeometryArrayTrait>);

#[pymethods]
impl PyChunkedGeometryArray {
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
        let arrow_chunks = self.0.array_refs();

        let array_reader = Box::new(ArrayIterator::new(arrow_chunks.into_iter().map(Ok), field));
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    /// Check for equality with other object.
    pub fn __eq__(&self, _other: &PyGeometryArray) -> bool {
        todo!()
        // self.0 == other.0
    }

    /// Access the item at a given index
    pub fn __getitem__(&self, _i: isize) -> PyGeoArrowResult<Option<PyGeometry>> {
        todo!()
        // // Handle negative indexes from the end
        // let i = if i < 0 {
        //     let i = self.0.len() as isize + i;
        //     if i < 0 {
        //         return Err(PyIndexError::new_err("Index out of range").into());
        //     }
        //     i as usize
        // } else {
        //     i as usize
        // };
        // if i >= self.0.len() {
        //     return Err(PyIndexError::new_err("Index out of range").into());
        // }

        // Ok(Some(PyGeometry(
        //     GeometryScalarArray::try_new(self.0.slice(i, 1)).unwrap(),
        // )))

        // // Ok(self.0.get(i).map(|geom| $return_type(geom.into())))
    }

    /// The number of rows
    pub fn __len__(&self) -> usize {
        todo!()
        // self.0.len()
    }

    /// Text representation
    pub fn __repr__(&self) -> String {
        todo!()
        // self.0.to_string()
    }

    /// Number of underlying chunks.
    pub fn num_chunks(&self) -> usize {
        self.0.num_chunks()
    }

    pub fn chunk(&self, i: usize) -> PyGeoArrowResult<PyGeometryArray> {
        let field = self.0.extension_field();
        let arrow_chunk = self.0.array_refs()[i].clone();
        Ok(from_arrow_array(&arrow_chunk, &field)?.into())
    }

    /// Convert to a list of single-chunked arrays.
    pub fn chunks(&self) -> PyGeoArrowResult<Vec<PyGeometryArray>> {
        let field = self.0.extension_field();
        let arrow_chunks = self.0.array_refs();
        let mut out = vec![];
        for chunk in arrow_chunks {
            out.push(from_arrow_array(&chunk, &field)?.into());
        }
        Ok(out)
    }
}
