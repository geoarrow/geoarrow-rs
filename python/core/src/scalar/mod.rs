use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct Point(pub(crate) geoarrow::scalar::OwnedPoint);

impl From<geoarrow::scalar::OwnedPoint> for Point {
    fn from(value: geoarrow::scalar::OwnedPoint) -> Self {
        Self(value)
    }
}
