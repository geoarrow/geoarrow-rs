use crate::array::ffi::FFIArrowArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::BooleanArray;
use crate::array::primitive::Float64Array;
use crate::error::WasmResult;
use crate::impl_geometry_array;
use arrow2::datatypes::Field;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);

impl From<&PointArray> for geoarrow::array::GeometryArray {
    fn from(value: &PointArray) -> Self {
        geoarrow::array::GeometryArray::Point(value.0.clone())
    }
}

impl_geometry_array!(PointArray);
