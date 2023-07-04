// TODO: better to not export at the top level?
// https://stackoverflow.com/a/31749071
#[macro_export]
macro_rules! impl_geometry_array {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen]
            pub fn area(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::area;
                Ok(Float64Array(area(self.into())?))
            }

            #[wasm_bindgen]
            pub fn center(&self) -> WasmResult<PointArray> {
                use geoarrow::algorithm::geo::center;
                Ok(PointArray(center(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn centroid(&self) -> WasmResult<PointArray> {
                use geoarrow::algorithm::geo::centroid;
                Ok(PointArray(centroid(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn convex_hull(&self) -> WasmResult<PolygonArray> {
                use geoarrow::algorithm::geo::convex_hull;
                Ok(PolygonArray(convex_hull(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn euclidean_length(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::euclidean_length;
                Ok(Float64Array(euclidean_length(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn geodesic_area(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::geodesic_area_unsigned;
                Ok(Float64Array(geodesic_area_unsigned(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn geodesic_area_signed(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::geodesic_area_signed;
                Ok(Float64Array(geodesic_area_signed(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn geodesic_length(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::geodesic_length;
                Ok(Float64Array(geodesic_length(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn haversine_length(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::haversine_length;
                Ok(Float64Array(haversine_length(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn is_empty(&self) -> WasmResult<BooleanArray> {
                use geoarrow::algorithm::geo::is_empty;
                Ok(BooleanArray(is_empty(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn signed_area(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::signed_area;
                Ok(Float64Array(signed_area(self.into())?))
            }

            #[wasm_bindgen]
            pub fn vincenty_length(&self) -> WasmResult<Float64Array> {
                use geoarrow::algorithm::geo::vincenty_length;
                Ok(Float64Array(vincenty_length(&self.into())?))
            }
        }
    };
}
