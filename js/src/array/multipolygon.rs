use crate::array::ffi::FFIArrowArray;
use crate::array::point::PointArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::BooleanArray;
use crate::array::primitive::Float64Array;
use crate::error::WasmResult;
use crate::impl_geometry_array;
use arrow2::datatypes::Field;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPolygonArray(pub(crate) geoarrow::array::MultiPolygonArray);

impl From<&MultiPolygonArray> for geoarrow::array::GeometryArray {
    fn from(value: &MultiPolygonArray) -> Self {
        geoarrow::array::GeometryArray::MultiPolygon(value.0.clone())
    }
}

impl_geometry_array!(MultiPolygonArray);
