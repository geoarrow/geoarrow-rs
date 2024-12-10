use crate::data::*;
use crate::error::WasmResult;
use crate::vector::*;
use arrow_wasm::data::Data;
use arrow_wasm::vector::Vector;
use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
use wasm_bindgen::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteUnsignedArea)]
            pub fn chamberlain_duquette_unsigned_area(&self) -> Data {
                Data::from_array(ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(
                    &self.0,
                ))
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteSignedArea)]
            pub fn chamberlain_duquette_signed_area(&self) -> Data {
                Data::from_array(ChamberlainDuquetteArea::chamberlain_duquette_signed_area(
                    &self.0,
                ))
            }
        }
    };
}

impl_alg!(PointData);
impl_alg!(LineStringData);
impl_alg!(PolygonData);
impl_alg!(MultiPointData);
impl_alg!(MultiLineStringData);
impl_alg!(MultiPolygonData);
impl_alg!(GeometryCollectionData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteUnsignedArea)]
            pub fn chamberlain_duquette_unsigned_area(&self) -> WasmResult<Vector> {
                let chunks = ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0)?
                    .chunk_refs();
                Ok(Vector::from_array_refs(chunks)?)
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteSignedArea)]
            pub fn chamberlain_duquette_signed_area(&self) -> WasmResult<Vector> {
                let chunks = ChamberlainDuquetteArea::chamberlain_duquette_signed_area(&self.0)?
                    .chunk_refs();
                Ok(Vector::from_array_refs(chunks)?)
            }
        }
    };
}

impl_vector!(PointVector);
impl_vector!(LineStringVector);
impl_vector!(PolygonVector);
impl_vector!(MultiPointVector);
impl_vector!(MultiLineStringVector);
impl_vector!(MultiPolygonVector);
impl_vector!(GeometryCollectionVector);
