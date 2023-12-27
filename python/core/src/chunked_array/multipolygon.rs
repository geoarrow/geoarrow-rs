use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct ChunkedMultiPolygonArray(
    pub(crate) geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>,
);

#[pymethods]
impl ChunkedMultiPolygonArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>> for ChunkedMultiPolygonArray {
    fn from(value: geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMultiPolygonArray> for geoarrow::chunked_array::ChunkedMultiPolygonArray<i32> {
    fn from(value: ChunkedMultiPolygonArray) -> Self {
        value.0
    }
}
