use crate::ffi::{from_py_array, to_py_array};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

/// An immutable array of MultiLineString geometries in WebAssembly memory using GeoArrow's
/// in-memory representation.
#[pyclass]
pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray<i32>);

impl From<geoarrow::array::MultiLineStringArray<i32>> for MultiLineStringArray {
    fn from(value: geoarrow::array::MultiLineStringArray<i32>) -> Self {
        Self(value)
    }
}

impl From<MultiLineStringArray> for geoarrow::array::MultiLineStringArray<i32> {
    fn from(value: MultiLineStringArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for MultiLineStringArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(MultiLineStringArray(
            arrow2_arr.as_ref().try_into().unwrap(),
        ))
    }
}

impl IntoPy<PyResult<PyObject>> for MultiLineStringArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_array_ref())
    }
}
