use crate::array::*;
use pyo3::prelude::*;
use pyo3::types::PyType;

/// An immutable array of WKB-formatted geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass]
pub struct WKBArray(pub(crate) geoarrow::array::WKBArray<i32>);

#[pymethods]
impl WKBArray {
    #[classmethod]
    fn from_arrow(_cls: &PyType, py: Python<'_>, ob: PyObject) -> Result<WKBArray, PyErr> {
        ob.extract(py)
    }

    fn to_point_array(&self) -> Result<PointArray, PyErr> {
        Ok(PointArray(self.0.clone().try_into().unwrap()))
    }

    fn to_line_string_array(&self) -> Result<LineStringArray, PyErr> {
        Ok(LineStringArray(self.0.clone().try_into().unwrap()))
    }

    fn to_polygon_array(&self) -> Result<PolygonArray, PyErr> {
        Ok(PolygonArray(self.0.clone().try_into().unwrap()))
    }

    fn to_multi_point_array(&self) -> Result<MultiPointArray, PyErr> {
        Ok(MultiPointArray(self.0.clone().try_into().unwrap()))
    }

    fn to_multi_line_string_array(&self) -> Result<MultiLineStringArray, PyErr> {
        Ok(MultiLineStringArray(self.0.clone().try_into().unwrap()))
    }

    fn to_multi_polygon_array(&self) -> Result<MultiPolygonArray, PyErr> {
        Ok(MultiPolygonArray(self.0.clone().try_into().unwrap()))
    }
}

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
