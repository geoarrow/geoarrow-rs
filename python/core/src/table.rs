use pyo3::prelude::*;

#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct GeoTable(pub(crate) geoarrow::table::GeoTable);
