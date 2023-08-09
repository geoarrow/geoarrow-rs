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
