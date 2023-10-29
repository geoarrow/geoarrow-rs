use crate::ffi::{from_py_array, to_py_array};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

/// An immutable array of WKB-formatted geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass]
pub struct WKBArray(pub(crate) geoarrow::array::WKBArray<i32>);

impl From<geoarrow::array::WKBArray<i32>> for WKBArray {
    fn from(value: geoarrow::array::WKBArray<i32>) -> Self {
        Self(value)
    }
}

impl From<WKBArray> for geoarrow::array::WKBArray<i32> {
    fn from(value: WKBArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for WKBArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(WKBArray(arrow2_arr.as_ref().try_into().unwrap()))
    }
}

impl IntoPy<PyResult<PyObject>> for WKBArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_array_ref())
    }
}
