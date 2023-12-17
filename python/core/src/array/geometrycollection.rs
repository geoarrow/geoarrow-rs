use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass]
pub struct GeometryCollectionArray(pub(crate) geoarrow::array::GeometryCollectionArray<i32>);

#[pymethods]
impl GeometryCollectionArray {
    #[classmethod]
    fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
        ob.extract()
    }
}

impl From<geoarrow::array::GeometryCollectionArray<i32>> for GeometryCollectionArray {
    fn from(value: geoarrow::array::GeometryCollectionArray<i32>) -> Self {
        Self(value)
    }
}

impl From<GeometryCollectionArray> for geoarrow::array::GeometryCollectionArray<i32> {
    fn from(value: GeometryCollectionArray) -> Self {
        value.0
    }
}
