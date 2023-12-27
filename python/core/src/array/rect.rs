use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct RectArray(pub(crate) geoarrow::array::RectArray);

#[pymethods]
impl RectArray {}

impl From<geoarrow::array::RectArray> for RectArray {
    fn from(value: geoarrow::array::RectArray) -> Self {
        Self(value)
    }
}

impl From<RectArray> for geoarrow::array::RectArray {
    fn from(value: RectArray) -> Self {
        value.0
    }
}
