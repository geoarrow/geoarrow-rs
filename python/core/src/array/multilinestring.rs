use pyo3::prelude::*;

/// An immutable array of MultiLineString geometries in WebAssembly memory using GeoArrow's
/// in-memory representation.
#[pyclass]
pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray<i32>);

impl From<geoarrow::array::MultiLineStringArray<i32>> for MultiLineStringArray {
    fn from(value: geoarrow::array::MultiLineStringArray<i32>) -> Self {
        Self(value)
    }
}

impl From<MultiLineStringArray> for geoarrow::array::MultiLineStringArray<i32> {
    fn from(value: MultiLineStringArray) -> Self {
        value.0
    }
}
