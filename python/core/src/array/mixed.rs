use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass]
pub struct MixedGeometryArray(pub(crate) geoarrow::array::MixedGeometryArray<i32>);

#[pymethods]
impl MixedGeometryArray {
    #[classmethod]
    fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
        ob.extract()
    }
}

impl From<geoarrow::array::MixedGeometryArray<i32>> for MixedGeometryArray {
    fn from(value: geoarrow::array::MixedGeometryArray<i32>) -> Self {
        Self(value)
    }
}

impl From<MixedGeometryArray> for geoarrow::array::MixedGeometryArray<i32> {
    fn from(value: MixedGeometryArray) -> Self {
        value.0
    }
}
