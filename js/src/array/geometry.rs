use crate::array::ffi::FFIArrowArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::BooleanArray;
use crate::array::primitive::FloatArray;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
};
use crate::broadcasting::{BroadcastableAffine, BroadcastableFloat};
use crate::error::WasmResult;
use crate::impl_geometry_array;
use crate::log;
use crate::TransformOrigin;
use arrow2::datatypes::Field;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct GeometryArray(pub(crate) geoarrow::array::GeometryArray);

impl_geometry_array!(GeometryArray);

#[wasm_bindgen]
impl GeometryArray {
    #[wasm_bindgen]
    pub fn from_point_array(arr: PointArray) -> Self {
        Self(geoarrow::array::GeometryArray::Point(arr.0))
    }

    #[wasm_bindgen]
    pub fn from_line_string_array(arr: LineStringArray) -> Self {
        Self(geoarrow::array::GeometryArray::LineString(arr.0))
    }

    #[wasm_bindgen]
    pub fn from_polygon_array(arr: PolygonArray) -> Self {
        Self(geoarrow::array::GeometryArray::Polygon(arr.0))
    }

    #[wasm_bindgen]
    pub fn from_multi_point_array(arr: MultiPointArray) -> Self {
        Self(geoarrow::array::GeometryArray::MultiPoint(arr.0))
    }

    #[wasm_bindgen]
    pub fn from_multi_line_string_array(arr: MultiLineStringArray) -> Self {
        Self(geoarrow::array::GeometryArray::MultiLineString(arr.0))
    }

    #[wasm_bindgen]
    pub fn from_multi_polygon_array(arr: MultiPolygonArray) -> Self {
        Self(geoarrow::array::GeometryArray::MultiPolygon(arr.0))
    }
}

impl From<&GeometryArray> for geoarrow::array::GeometryArray {
    fn from(value: &GeometryArray) -> Self {
        value.0.clone()
    }
}
