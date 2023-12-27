use pyo3::prelude::*;
use pyo3::types::PyType;

/// An immutable array of Polygon geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass(module = "geoarrow.rust.core.rust")]
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
