use crate::array::primitive::BooleanArray;
use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use crate::error::WasmResult;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of WKB-formatted geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct WKBArray(pub(crate) geoarrow::array::WKBArray<i32>);

#[wasm_bindgen]
impl WKBArray {
    #[wasm_bindgen(constructor)]
    pub fn new(values: Vec<u8>, offsets: Vec<i32>, validity: Option<BooleanArray>) -> Self {
        let binary_array = arrow2::array::BinaryArray::new(
            arrow2::datatypes::DataType::Binary,
            vec_to_offsets(offsets),
            values.into(),
            validity.map(|validity| validity.0.values().clone()),
        );
        Self(geoarrow::array::WKBArray::new(binary_array))
    }

    /// Convert this WKBArray into a PointArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBArray, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoPointArray)]
    pub fn into_point_array(self) -> WasmResult<PointArray> {
        let arr: geoarrow::array::PointArray = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBArray into a LineStringArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBArray, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoLineStringArray)]
    pub fn into_line_string_array(self) -> WasmResult<LineStringArray> {
        let arr: geoarrow::array::LineStringArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBArray into a PolygonArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBArray, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoPolygonArray)]
    pub fn into_polygon_array(self) -> WasmResult<PolygonArray> {
        let arr: geoarrow::array::PolygonArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBArray into a MultiPointArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBArray, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoMultiPointArray)]
    pub fn into_multi_point_array(self) -> WasmResult<MultiPointArray> {
        let arr: geoarrow::array::MultiPointArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBArray into a MultiLineStringArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBArray, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoMultiLineStringArray)]
    pub fn into_multi_line_string_array(self) -> WasmResult<MultiLineStringArray> {
        let arr: geoarrow::array::MultiLineStringArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }

    /// Convert this WKBArray into a MultiPolygonArray
    ///
    /// ## Memory management
    ///
    /// This operation consumes and neuters the existing WKBArray, so it will no longer be valid
    /// and the original wkb array's memory does not need to be freed manually.
    #[wasm_bindgen(js_name = intoMultiPolygonArray)]
    pub fn into_multi_polygon_array(self) -> WasmResult<MultiPolygonArray> {
        let arr: geoarrow::array::MultiPolygonArray<i32> = self.0.try_into().unwrap();
        Ok(arr.into())
    }
}
