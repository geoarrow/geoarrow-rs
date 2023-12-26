use pyo3::prelude::*;

#[pyclass]
pub struct ChunkedWKBArray(pub(crate) geoarrow::chunked_array::ChunkedWKBArray<i32>);

#[pymethods]
impl ChunkedWKBArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedWKBArray<i32>> for ChunkedWKBArray {
    fn from(value: geoarrow::chunked_array::ChunkedWKBArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedWKBArray> for geoarrow::chunked_array::ChunkedWKBArray<i32> {
    fn from(value: ChunkedWKBArray) -> Self {
        value.0
    }
}
