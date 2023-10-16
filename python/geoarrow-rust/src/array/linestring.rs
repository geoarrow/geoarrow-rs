use crate::ffi::{from_py_array, to_py_array};
use geoarrow::GeometryArrayTrait;
use pyo3::prelude::*;

/// An immutable array of LineString geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass]
pub struct LineStringArray(pub(crate) geoarrow::array::LineStringArray<i32>);

impl From<geoarrow::array::LineStringArray<i32>> for LineStringArray {
    fn from(value: geoarrow::array::LineStringArray<i32>) -> Self {
        Self(value)
    }
}

impl From<LineStringArray> for geoarrow::array::LineStringArray<i32> {
    fn from(value: LineStringArray) -> Self {
        value.0
    }
}

impl<'a> FromPyObject<'a> for LineStringArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let arrow2_arr = from_py_array(ob)?;
        Ok(LineStringArray(arrow2_arr.as_ref().try_into().unwrap()))
    }
}

impl IntoPy<PyResult<PyObject>> for LineStringArray {
    fn into_py(self, py: Python<'_>) -> PyResult<PyObject> {
        to_py_array(py, self.0.into_array_ref())
    }
}
