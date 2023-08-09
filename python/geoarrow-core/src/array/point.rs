use pyo3::prelude::*;

#[pyclass]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);

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
