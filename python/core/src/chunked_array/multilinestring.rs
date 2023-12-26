use pyo3::prelude::*;

#[pyclass]
pub struct ChunkedMultiLineStringArray(
    pub(crate) geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>,
);

#[pymethods]
impl ChunkedMultiLineStringArray {
    // #[classmethod]
    // fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
    //     ob.extract()
    // }
}

impl From<geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>>
    for ChunkedMultiLineStringArray
{
    fn from(value: geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMultiLineStringArray>
    for geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>
{
    fn from(value: ChunkedMultiLineStringArray) -> Self {
        value.0
    }
}
