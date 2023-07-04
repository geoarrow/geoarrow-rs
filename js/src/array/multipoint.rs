use wasm_bindgen::prelude::*;

use crate::array::point::PointArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::BooleanArray;
use crate::array::primitive::Float64Array;
use crate::error::WasmResult;
use crate::impl_geometry_array;

#[wasm_bindgen]
pub struct MultiPointArray(pub(crate) geoarrow::array::MultiPointArray);

impl From<&MultiPointArray> for geoarrow::array::GeometryArray {
    fn from(value: &MultiPointArray) -> Self {
        geoarrow::array::GeometryArray::MultiPoint(value.0.clone())
    }
}

impl_geometry_array!(MultiPointArray);
