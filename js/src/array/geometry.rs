use crate::array::ffi::FFIArrowArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::FloatArray;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
};
use crate::broadcasting::{BroadcastableAffine, BroadcastableFloat};
use crate::error::WasmResult;
use crate::impl_geometry_array;
use crate::log;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use crate::TransformOrigin;
use arrow2::datatypes::Field;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub enum GeometryType {
    Point = 0,
    LineString = 1,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
}

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

    #[wasm_bindgen]
    pub fn geometry_type(&self) -> GeometryType {
        match self.0 {
            geoarrow::array::GeometryArray::Point(_) => GeometryType::Point,
            geoarrow::array::GeometryArray::LineString(_) => GeometryType::LineString,
            geoarrow::array::GeometryArray::Polygon(_) => GeometryType::Polygon,
            geoarrow::array::GeometryArray::MultiPoint(_) => GeometryType::MultiPoint,
            geoarrow::array::GeometryArray::MultiLineString(_) => GeometryType::MultiLineString,
            geoarrow::array::GeometryArray::MultiPolygon(_) => GeometryType::MultiPolygon,
            geoarrow::array::GeometryArray::WKB(_) => unimplemented!(),
        }
    }
}

impl From<&GeometryArray> for geoarrow::array::GeometryArray {
    fn from(value: &GeometryArray) -> Self {
        value.0.clone()
    }
}
