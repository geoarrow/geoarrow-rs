use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct ChunkedMultiPointArray(pub(crate) geoarrow::chunked_array::ChunkedMultiPointArray<i32>);

#[pymethods]
impl ChunkedMultiPointArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedMultiPointArray<i32>> for ChunkedMultiPointArray {
    fn from(value: geoarrow::chunked_array::ChunkedMultiPointArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMultiPointArray> for geoarrow::chunked_array::ChunkedMultiPointArray<i32> {
    fn from(value: ChunkedMultiPointArray) -> Self {
        value.0
    }
}
