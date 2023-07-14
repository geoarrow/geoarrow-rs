// TODO: better to not export at the top level?
// https://stackoverflow.com/a/31749071
#[macro_export]
macro_rules! impl_geometry_array {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[cfg(feature = "geodesy")]
            #[wasm_bindgen]
            pub fn reproject_rs(
                &self,
                definition: &str,
                direction: ReprojectDirection,
            ) -> WasmResult<GeometryArray> {
                use geoarrow::algorithm::geodesy::reproject;
                Ok(GeometryArray(reproject(
                    &self.into(),
                    definition,
                    direction.into(),
                )?))
            }
        }
    };
}
