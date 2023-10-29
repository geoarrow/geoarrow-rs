use crate::ffi::{from_py_array, to_py_array};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

/// An immutable array of MultiPolygon geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass]
pub struct MultiPolygonArray(pub(crate) geoarrow::array::MultiPolygonArray<i32>);

impl From<geoarrow::array::MultiPolygonArray<i32>> for MultiPolygonArray {
    fn from(value: geoarrow::array::MultiPolygonArray<i32>) -> Self {
        Self(value)
    }
}

impl From<MultiPolygonArray> for geoarrow::array::MultiPolygonArray<i32> {
    fn from(value: MultiPolygonArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for MultiPolygonArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(MultiPolygonArray(arrow2_arr.as_ref().try_into().unwrap()))
    }
}

impl IntoPy<PyResult<PyObject>> for MultiPolygonArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_array_ref())
    }
}
