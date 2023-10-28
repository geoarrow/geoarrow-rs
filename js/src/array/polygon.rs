use crate::array::CoordBuffer;
use crate::array::GeometryArray;
use crate::error::WasmResult;
use crate::impl_geometry_array;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of Polygon geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct PolygonArray(pub(crate) geoarrow::array::PolygonArray<i32>);

impl_geometry_array!(PolygonArray);

#[wasm_bindgen]
impl PolygonArray {
    #[wasm_bindgen(constructor)]
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: Vec<i32>,
        ring_offsets: Vec<i32>,
        // validity: Option<BooleanArray>,
    ) -> Self {
        Self(geoarrow::array::PolygonArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            vec_to_offsets(ring_offsets),
            None,
        ))
    }
}

impl From<&PolygonArray> for geoarrow::array::GeometryArray<i32> {
    fn from(value: &PolygonArray) -> Self {
        geoarrow::array::GeometryArray::Polygon(value.0.clone())
    }
}

impl From<geoarrow::array::PolygonArray<i32>> for PolygonArray {
    fn from(value: geoarrow::array::PolygonArray<i32>) -> Self {
        Self(value)
    }
}
