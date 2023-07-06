// TODO: better to not export at the top level?
// https://stackoverflow.com/a/31749071
#[macro_export]
macro_rules! impl_geometry_array {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen]
            pub fn affine_transform(
                &self,
                transform: BroadcastableAffine,
            ) -> WasmResult<GeometryArray> {
                use geoarrow::algorithm::geo::affine_transform;
                Ok(GeometryArray(affine_transform(&self.into(), transform.0)?))
            }

            #[wasm_bindgen]
            pub fn area(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::area;
                Ok(FloatArray(area(self.into())?))
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
            pub fn euclidean_length(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::euclidean_length;
                Ok(FloatArray(euclidean_length(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn geodesic_area(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::geodesic_area_unsigned;
                Ok(FloatArray(geodesic_area_unsigned(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn geodesic_area_signed(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::geodesic_area_signed;
                Ok(FloatArray(geodesic_area_signed(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn geodesic_length(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::geodesic_length;
                Ok(FloatArray(geodesic_length(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn haversine_length(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::haversine_length;
                Ok(FloatArray(haversine_length(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn is_empty(&self) -> WasmResult<BooleanArray> {
                use geoarrow::algorithm::geo::is_empty;
                Ok(BooleanArray(is_empty(&self.into())?))
            }

            #[wasm_bindgen]
            pub fn rotate(
                &self,
                angle: BroadcastableFloat,
                origin: TransformOrigin,
            ) -> WasmResult<GeometryArray> {
                use geoarrow::algorithm::geo::rotate;
                Ok(GeometryArray(rotate(&self.into(), angle.0, origin.0)?))
            }

            #[wasm_bindgen]
            pub fn scale(
                &self,
                xfact: BroadcastableFloat,
                yfact: BroadcastableFloat,
                origin: TransformOrigin,
            ) -> WasmResult<GeometryArray> {
                use geoarrow::algorithm::geo::scale;
                Ok(GeometryArray(scale(
                    &self.into(),
                    xfact.0,
                    yfact.0,
                    origin.0,
                )?))
            }

            #[wasm_bindgen]
            pub fn signed_area(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::signed_area;
                Ok(FloatArray(signed_area(self.into())?))
            }

            #[wasm_bindgen]
            pub fn skew(
                &self,
                x_degrees: BroadcastableFloat,
                y_degrees: BroadcastableFloat,
                origin: TransformOrigin,
            ) -> WasmResult<GeometryArray> {
                use geoarrow::algorithm::geo::skew;
                Ok(GeometryArray(skew(
                    &self.into(),
                    x_degrees.0,
                    y_degrees.0,
                    origin.0,
                )?))
            }

            #[wasm_bindgen]
            pub fn to_ffi(&self) -> FFIArrowArray {
                let arrow_array = self.0.clone().into_boxed_arrow();
                let field = Field::new("", arrow_array.data_type().clone(), true);
                FFIArrowArray::new(&field, arrow_array)
            }

            #[wasm_bindgen]
            pub fn translate(
                &self,
                x_offset: BroadcastableFloat,
                y_offset: BroadcastableFloat,
            ) -> WasmResult<GeometryArray> {
                use geoarrow::algorithm::geo::translate;
                log!("{:?}", &self.0);
                Ok(GeometryArray(translate(
                    &self.into(),
                    x_offset.0,
                    y_offset.0,
                )?))
            }

            #[wasm_bindgen]
            pub fn vincenty_length(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::vincenty_length;
                Ok(FloatArray(vincenty_length(&self.into())?))
            }
        }
    };
}
