use crate::array::ffi::FFIArrowArray;
use crate::array::point::PointArray;
use crate::array::primitive::BooleanArray;
use crate::array::primitive::Float64Array;
use crate::error::WasmResult;
use crate::impl_geometry_array;
use arrow2::datatypes::Field;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct PolygonArray(pub(crate) geoarrow::array::PolygonArray);

impl From<&PolygonArray> for geoarrow::array::GeometryArray {
    fn from(value: &PolygonArray) -> Self {
        geoarrow::array::GeometryArray::Polygon(value.0.clone())
    }
}

impl_geometry_array!(PolygonArray);
