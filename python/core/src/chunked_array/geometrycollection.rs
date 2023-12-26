use pyo3::prelude::*;

#[pyclass]
pub struct ChunkedGeometryCollectionArray(
    pub(crate) geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>,
);

#[pymethods]
impl ChunkedGeometryCollectionArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>>
    for ChunkedGeometryCollectionArray
{
    fn from(value: geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedGeometryCollectionArray>
    for geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>
{
    fn from(value: ChunkedGeometryCollectionArray) -> Self {
        value.0
    }
}
