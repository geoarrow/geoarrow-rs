use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct ChunkedPointArray(pub(crate) geoarrow::chunked_array::ChunkedPointArray);

#[pymethods]
impl ChunkedPointArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedPointArray> for ChunkedPointArray {
    fn from(value: geoarrow::chunked_array::ChunkedPointArray) -> Self {
        Self(value)
    }
}

impl From<ChunkedPointArray> for geoarrow::chunked_array::ChunkedPointArray {
    fn from(value: ChunkedPointArray) -> Self {
        value.0
    }
}
