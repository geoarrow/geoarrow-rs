use wasm_bindgen::prelude::*;

use crate::array::point::PointArray;
use crate::array::primitive::BooleanArray;
use crate::array::primitive::Float64Array;
use crate::error::WasmResult;
use crate::impl_geometry_array;

#[wasm_bindgen]
pub struct PolygonArray(pub(crate) geoarrow::array::PolygonArray);

impl From<&PolygonArray> for geoarrow::array::GeometryArray {
    fn from(value: &PolygonArray) -> Self {
        geoarrow::array::GeometryArray::Polygon(value.0.clone())
    }
}

impl_geometry_array!(PolygonArray);
