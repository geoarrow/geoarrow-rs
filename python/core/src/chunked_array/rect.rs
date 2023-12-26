use pyo3::prelude::*;

#[pyclass]
pub struct ChunkedRectArray(pub(crate) geoarrow::chunked_array::ChunkedRectArray);

#[pymethods]
impl ChunkedRectArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedRectArray> for ChunkedRectArray {
    fn from(value: geoarrow::chunked_array::ChunkedRectArray) -> Self {
        Self(value)
    }
}

impl From<ChunkedRectArray> for geoarrow::chunked_array::ChunkedRectArray {
    fn from(value: ChunkedRectArray) -> Self {
        value.0
    }
}
