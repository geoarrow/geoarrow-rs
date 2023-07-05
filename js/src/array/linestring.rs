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
pub struct LineStringArray(pub(crate) geoarrow::array::LineStringArray);

impl From<&LineStringArray> for geoarrow::array::GeometryArray {
    fn from(value: &LineStringArray) -> Self {
        geoarrow::array::GeometryArray::LineString(value.0.clone())
    }
}

impl_geometry_array!(LineStringArray);
