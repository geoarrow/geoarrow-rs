use crate::data::*;
use crate::vector::*;
use arrow_wasm::arrow1::data::Float64Data;
use arrow_wasm::arrow1::vector::Float64Vector;
use wasm_bindgen::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteUnsignedArea)]
            pub fn chamberlain_duquette_unsigned_area(&self) -> Float64Data {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0).into()
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteSignedArea)]
            pub fn chamberlain_duquette_signed_area(&self) -> Float64Data {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                ChamberlainDuquetteArea::chamberlain_duquette_signed_area(&self.0).into()
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
impl_alg!(MixedGeometryData);
impl_alg!(GeometryCollectionData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteUnsignedArea)]
            pub fn chamberlain_duquette_unsigned_area(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                Float64Vector::new(
                    ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(&self.0)
                        .unwrap()
                        .into_inner(),
                )
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            #[wasm_bindgen(js_name = chamberlainDuquetteSignedArea)]
            pub fn chamberlain_duquette_signed_area(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                Float64Vector::new(
                    ChamberlainDuquetteArea::chamberlain_duquette_signed_area(&self.0)
                        .unwrap()
                        .into_inner(),
                )
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
impl_vector!(MixedGeometryVector);
impl_vector!(GeometryCollectionVector);
