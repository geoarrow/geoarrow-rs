use crate::ffi::{from_py_array, to_py_array};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

/// An immutable array of MultiPoint geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass]
pub struct MultiPointArray(pub(crate) geoarrow::array::MultiPointArray<i32>);

impl From<geoarrow::array::MultiPointArray<i32>> for MultiPointArray {
    fn from(value: geoarrow::array::MultiPointArray<i32>) -> Self {
        Self(value)
    }
}

impl From<MultiPointArray> for geoarrow::array::MultiPointArray<i32> {
    fn from(value: MultiPointArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for MultiPointArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(MultiPointArray(arrow2_arr.as_ref().try_into().unwrap()))
    }
}

impl IntoPy<PyResult<PyObject>> for MultiPointArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_array_ref())
    }
}
