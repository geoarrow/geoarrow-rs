// use wasm_bindgen::prelude::*;

// use crate::data::JsGeoArrowData;
// use crate::data_type::JsGeoArrowType;
// use crate::error::WasmResult;

// #[wasm_bindgen]
// pub fn from_wkb(
//     input: &JsGeoArrowData,
//     to_type: Option<JsGeoArrowType>,
// ) -> WasmResult<JsGeoArrowData> {
//     let input = input.inner();
//     let input_metadata = input.data_type().metadata().clone();
//     let to_type = to_type
//         .map(|x| x.into_inner())
//         .unwrap_or(GeometryType::new(input_metadata).into());

//     let x = geoarrow_array::cast::from_wkb(arr.inner(), to_type.inner())?;
//     let casted_array = geoarrow_array
//         .cast_to_type(target_type)
//         .map_err(|e| wasm_bindgen::JsError::new(&e.to_string()))?;

//     Ok(JsGeoArrowData(Arc::from(casted_array)))
// }
