use crate::ffi::{from_py_array, to_py_array};
use geoarrow::GeometryArrayTrait;
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

impl<'a> FromPyObject<'a> for PointArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(PointArray(arrow2_arr.as_ref().try_into().unwrap()))
    }
}

impl IntoPy<PyResult<PyObject>> for PointArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_array_ref())
    }
}
