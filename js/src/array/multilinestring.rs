use geoarrow::array::GeometryArray;
use wasm_bindgen::prelude::*;

use crate::array::point::PointArray;
use crate::array::polygon::PolygonArray;
use crate::array::primitive::Float64Array;
use crate::error::WasmResult;

#[wasm_bindgen]
pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray);

#[wasm_bindgen]
impl MultiLineStringArray {
    #[wasm_bindgen]
    pub fn area(&self) -> WasmResult<Float64Array> {
        use geoarrow::algorithm::geo::area;
        let out = area(GeometryArray::MultiLineString(self.0.clone()))?;
        Ok(Float64Array(out))
    }

    #[wasm_bindgen]
    pub fn center(&self) -> WasmResult<PointArray> {
        use geoarrow::algorithm::geo::center;
        let out = center(&GeometryArray::MultiLineString(self.0.clone()))?;
        Ok(PointArray(out))
    }

    #[wasm_bindgen]
    pub fn centroid(&self) -> WasmResult<PointArray> {
        use geoarrow::algorithm::geo::centroid;
        let out = centroid(&GeometryArray::MultiLineString(self.0.clone()))?;
        Ok(PointArray(out))
    }

    #[wasm_bindgen]
    pub fn convex_hull(&self) -> WasmResult<PolygonArray> {
        use geoarrow::algorithm::geo::convex_hull;
        let out = convex_hull(&GeometryArray::MultiLineString(self.0.clone()))?;
        Ok(PolygonArray(out))
    }

    #[wasm_bindgen]
    pub fn signed_area(&self) -> WasmResult<Float64Array> {
        use geoarrow::algorithm::geo::signed_area;
        let out = signed_area(GeometryArray::MultiLineString(self.0.clone()))?;
        Ok(Float64Array(out))
    }
}
