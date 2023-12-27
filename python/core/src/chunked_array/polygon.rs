use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct ChunkedPolygonArray(pub(crate) geoarrow::chunked_array::ChunkedPolygonArray<i32>);

#[pymethods]
impl ChunkedPolygonArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedPolygonArray<i32>> for ChunkedPolygonArray {
    fn from(value: geoarrow::chunked_array::ChunkedPolygonArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedPolygonArray> for geoarrow::chunked_array::ChunkedPolygonArray<i32> {
    fn from(value: ChunkedPolygonArray) -> Self {
        value.0
    }
}
