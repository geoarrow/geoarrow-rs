use crate::array::primitive::BooleanArray;
use crate::array::CoordBuffer;
use crate::array::GeometryArray;
use crate::error::WasmResult;
use crate::impl_geometry_array;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of LineString geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct LineStringArray(pub(crate) geoarrow::array::LineStringArray<i32>);

impl_geometry_array!(LineStringArray);

#[wasm_bindgen]
impl LineStringArray {
    #[wasm_bindgen(constructor)]
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: Vec<i32>,
        validity: Option<BooleanArray>,
    ) -> Self {
        Self(geoarrow::array::LineStringArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            validity.map(|validity| validity.0.values().clone()),
        ))
    }
}

impl From<&LineStringArray> for geoarrow::array::GeometryArray<i32> {
    fn from(value: &LineStringArray) -> Self {
        geoarrow::array::GeometryArray::LineString(value.0.clone())
    }
}

impl From<geoarrow::array::LineStringArray<i32>> for LineStringArray {
    fn from(value: geoarrow::array::LineStringArray<i32>) -> Self {
        Self(value)
    }
}
