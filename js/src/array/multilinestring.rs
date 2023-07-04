use wasm_bindgen::prelude::*;

use crate::array::point::PointArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::Float64Array;
use crate::error::WasmResult;
use crate::impl_geometry_array;

#[wasm_bindgen]
pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray);

impl From<&MultiLineStringArray> for geoarrow::array::GeometryArray {
    fn from(value: &MultiLineStringArray) -> Self {
        geoarrow::array::GeometryArray::MultiLineString(value.0.clone())
    }
}

impl_geometry_array!(MultiLineStringArray);
