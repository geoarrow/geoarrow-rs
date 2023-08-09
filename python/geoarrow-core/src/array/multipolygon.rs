use pyo3::prelude::*;

/// An immutable array of MultiPolygon geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[pyclass]
pub struct MultiPolygonArray(pub(crate) geoarrow::array::MultiPolygonArray<i32>);

impl From<geoarrow::array::MultiPolygonArray<i32>> for MultiPolygonArray {
    fn from(value: geoarrow::array::MultiPolygonArray<i32>) -> Self {
        Self(value)
    }
}

impl From<MultiPolygonArray> for geoarrow::array::MultiPolygonArray<i32> {
    fn from(value: MultiPolygonArray) -> Self {
        value.0
    }
}
