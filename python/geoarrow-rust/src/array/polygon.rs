use crate::ffi::{from_py_array, to_py_array};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;
use pyo3::types::PyType;

/// An immutable array of Polygon geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass]
pub struct PolygonArray(pub(crate) geoarrow::array::PolygonArray<i32>);

#[pymethods]
impl PolygonArray {
    #[classmethod]
    fn from_arrow(_cls: &PyType, ob: &PyAny) -> PyResult<Self> {
        ob.extract()
    }
}

impl From<geoarrow::array::PolygonArray<i32>> for PolygonArray {
    fn from(value: geoarrow::array::PolygonArray<i32>) -> Self {
        Self(value)
    }
}

impl From<PolygonArray> for geoarrow::array::PolygonArray<i32> {
    fn from(value: PolygonArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for PolygonArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(PolygonArray(arrow2_arr.as_ref().try_into().unwrap()))
    }
}

impl IntoPy<PyResult<PyObject>> for PolygonArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_boxed_arrow())
    }
}
