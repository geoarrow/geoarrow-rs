use pyo3::prelude::*;

#[pyclass]
pub struct ChunkedLineStringArray(pub(crate) geoarrow::chunked_array::ChunkedLineStringArray<i32>);

#[pymethods]
impl ChunkedLineStringArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedLineStringArray<i32>> for ChunkedLineStringArray {
    fn from(value: geoarrow::chunked_array::ChunkedLineStringArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedLineStringArray> for geoarrow::chunked_array::ChunkedLineStringArray<i32> {
    fn from(value: ChunkedLineStringArray) -> Self {
        value.0
    }
}
