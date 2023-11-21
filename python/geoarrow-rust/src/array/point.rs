use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);

#[pymethods]
impl PointArray {
    #[classmethod]
    fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
        ob.extract()
    }
}

impl From<geoarrow::array::PointArray> for PointArray {
    fn from(value: geoarrow::array::PointArray) -> Self {
        Self(value)
    }
}

impl From<PointArray> for geoarrow::array::PointArray {
    fn from(value: PointArray) -> Self {
        value.0
    }
}

