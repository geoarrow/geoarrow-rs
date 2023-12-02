use crate::array::CoordBuffer;
use crate::error::WasmResult;
use crate::impl_geometry_array;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of MultiPolygon geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct MultiPolygonArray(pub(crate) geoarrow::array::MultiPolygonArray<i32>);

impl_geometry_array!(MultiPolygonArray);

#[wasm_bindgen]
impl MultiPolygonArray {
    #[wasm_bindgen(constructor)]
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: Vec<i32>,
        polygon_offsets: Vec<i32>,
        ring_offsets: Vec<i32>,
    ) -> Self {
        Self(geoarrow::array::MultiPolygonArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            vec_to_offsets(polygon_offsets),
            vec_to_offsets(ring_offsets),
            None,
        ))
    }
}

impl From<geoarrow::array::MultiPolygonArray<i32>> for MultiPolygonArray {
    fn from(value: geoarrow::array::MultiPolygonArray<i32>) -> Self {
        Self(value)
    }
}
