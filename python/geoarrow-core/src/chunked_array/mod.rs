use std::sync::Arc;

use geoarrow::array::from_arrow_array;
use geoarrow::chunked_array::ChunkedGeometryArrayTrait;
use geoarrow::scalar::GeometryScalar;
use pyo3::exceptions::PyIndexError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use pyo3_arrow::ffi::{to_stream_pycapsule, ArrayIterator};

use crate::array::PyGeometryArray;
use crate::error::PyGeoArrowResult;
use crate::scalar::PyGeometry;

#[pyclass(
    module = "geoarrow.rust.core._rust",
    name = "ChunkedGeometryArray",
    subclass
)]
pub struct PyChunkedGeometryArray(pub(crate) Arc<dyn ChunkedGeometryArrayTrait>);

impl PyChunkedGeometryArray {
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &dyn ChunkedGeometryArrayTrait {
        self.0.as_ref()
    }
}

#[pymethods]
impl PyChunkedGeometryArray {
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

    // /// Check for equality with other object.
    // pub fn __eq__(&self, _other: &PyGeometryArray) -> bool {
    //     self.0 == other.0
    // }

    pub fn __getitem__(&self, i: isize) -> PyGeoArrowResult<Option<PyGeometry>> {
        // Handle negative indexes from the end
        let i = if i < 0 {
            let i = self.0.len() as isize + i;
            if i < 0 {
                return Err(PyIndexError::new_err("Index out of range").into());
            }
            i as usize
        } else {
            i as usize
        };
        if i >= self.0.len() {
            return Err(PyIndexError::new_err("Index out of range").into());
        }

        let sliced = self.0.slice(i, 1)?;
        let geom_chunks = sliced.geometry_chunks();
        assert_eq!(geom_chunks.len(), 1);
        Ok(Some(PyGeometry(
            GeometryScalar::try_new(geom_chunks[0].clone()).unwrap(),
        )))
    }

    pub fn __len__(&self) -> usize {
        self.0.len()
    }

    pub fn __repr__(&self) -> String {
        // self.0.to_string()
        "geoarrow.rust.core.ChunkedGeometryArray".to_string()
    }

    pub fn num_chunks(&self) -> usize {
        self.0.num_chunks()
    }

    pub fn chunk(&self, i: usize) -> PyGeoArrowResult<PyGeometryArray> {
        let field = self.0.extension_field();
        let arrow_chunk = self.0.array_refs()[i].clone();
        Ok(from_arrow_array(&arrow_chunk, &field)?.into())
    }

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
