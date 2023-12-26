use pyo3::prelude::*;

#[pyclass]
pub struct ChunkedMixedGeometryArray(
    pub(crate) geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>,
);

#[pymethods]
impl ChunkedMixedGeometryArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>> for ChunkedMixedGeometryArray {
    fn from(value: geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMixedGeometryArray> for geoarrow::chunked_array::ChunkedMixedGeometryArray<i32> {
    fn from(value: ChunkedMixedGeometryArray) -> Self {
        value.0
    }
}
