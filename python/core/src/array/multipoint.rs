use pyo3::prelude::*;

/// An immutable array of MultiPoint geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct MultiPointArray(pub(crate) geoarrow::array::MultiPointArray<i32>);

impl From<geoarrow::array::MultiPointArray<i32>> for MultiPointArray {
    fn from(value: geoarrow::array::MultiPointArray<i32>) -> Self {
        Self(value)
    }
}

impl From<MultiPointArray> for geoarrow::array::MultiPointArray<i32> {
    fn from(value: MultiPointArray) -> Self {
        value.0
    }
}
