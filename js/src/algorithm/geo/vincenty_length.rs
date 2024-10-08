use crate::data::*;
use crate::error::WasmResult;
use arrow_wasm::data::Data;
use geoarrow::algorithm::geo::VincentyLength;
use wasm_bindgen::prelude::*;

macro_rules! impl_vincenty_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Determine the length of a geometry using [Vincenty’s formulae].
            ///
            /// [Vincenty’s formulae]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
            #[wasm_bindgen(js_name = vincentyLength)]
            pub fn vincenty_length(&self) -> WasmResult<Data> {
                Ok(Data::from_array(VincentyLength::vincenty_length(&self.0)?))
            }
        }
    };
}

impl_vincenty_length!(PointData);
impl_vincenty_length!(MultiPointData);
impl_vincenty_length!(LineStringData);
impl_vincenty_length!(MultiLineStringData);
