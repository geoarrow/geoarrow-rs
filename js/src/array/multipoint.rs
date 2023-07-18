use crate::array::primitive::BooleanArray;
use crate::array::CoordBuffer;
use crate::array::GeometryArray;
use crate::error::WasmResult;
use crate::impl_geometry_array;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of MultiPoint geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct MultiPointArray(pub(crate) geoarrow::array::MultiPointArray<i32>);

impl_geometry_array!(MultiPointArray);

#[wasm_bindgen]
impl MultiPointArray {
    #[wasm_bindgen(constructor)]
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: Vec<i32>,
        validity: Option<BooleanArray>,
    ) -> Self {
        Self(geoarrow::array::MultiPointArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            validity.map(|validity| validity.0.values().clone()),
        ))
    }
}

impl From<&MultiPointArray> for geoarrow::array::GeometryArray<i32> {
    fn from(value: &MultiPointArray) -> Self {
        geoarrow::array::GeometryArray::MultiPoint(value.0.clone())
    }
}

impl From<geoarrow::array::MultiPointArray<i32>> for MultiPointArray {
    fn from(value: geoarrow::array::MultiPointArray<i32>) -> Self {
        Self(value)
    }
}
