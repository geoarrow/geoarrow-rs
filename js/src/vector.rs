use std::sync::Arc;

use geoarrow_array::{GeoArrowArray, GeoArrowType};
use wasm_bindgen::prelude::*;

/// An immutable vector (chunked array) of geometries stored in WebAssembly memory using GeoArrow's
/// in-memory representation.
#[wasm_bindgen(js_name = GeoArrowVector)]
pub struct JsGeoArrowVector {
    _chunks: Vec<Arc<dyn GeoArrowArray>>,
    _data_type: GeoArrowType,
}
