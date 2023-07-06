use crate::array::ffi::FFIArrowArray;
use crate::array::point::PointArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::BooleanArray;
use crate::array::primitive::FloatArray;
use crate::array::CoordBuffer;
use crate::array::GeometryArray;
use crate::broadcasting::{BroadcastableAffine, BroadcastableFloat};
use crate::error::WasmResult;
use crate::impl_geometry_array;
use crate::log;
use crate::utils::vec_to_offsets;
use crate::TransformOrigin;
use arrow2::datatypes::Field;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPolygonArray(pub(crate) geoarrow::array::MultiPolygonArray);

impl_geometry_array!(MultiPolygonArray);

#[wasm_bindgen]
impl MultiPolygonArray {
    #[wasm_bindgen(constructor)]
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: Vec<i32>,
        polygon_offsets: Vec<i32>,
        ring_offsets: Vec<i32>,
        validity: Option<BooleanArray>,
    ) -> Self {
        Self(geoarrow::array::MultiPolygonArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            vec_to_offsets(polygon_offsets),
            vec_to_offsets(ring_offsets),
            validity.map(|validity| validity.0.values().clone()),
        ))
    }
}
impl From<&MultiPolygonArray> for geoarrow::array::GeometryArray {
    fn from(value: &MultiPolygonArray) -> Self {
        geoarrow::array::GeometryArray::MultiPolygon(value.0.clone())
    }
}
