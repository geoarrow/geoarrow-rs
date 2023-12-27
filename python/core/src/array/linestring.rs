use pyo3::prelude::*;

/// An immutable array of LineString geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass(module = "geoarrow.rust.core.rust")]
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
