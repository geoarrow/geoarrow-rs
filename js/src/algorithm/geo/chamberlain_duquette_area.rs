use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculate the unsigned approximate geodesic area of a `Geometry`.
            #[wasm_bindgen]
            pub fn chamberlain_duquette_unsigned_area(&self) -> FloatArray {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                FloatArray(ChamberlainDuquetteArea::chamberlain_duquette_unsigned_area(
                    &self.0,
                ))
            }

            /// Calculate the signed approximate geodesic area of a `Geometry`.
            #[wasm_bindgen]
            pub fn chamberlain_duquette_signed_area(&self) -> FloatArray {
                use geoarrow::algorithm::geo::ChamberlainDuquetteArea;
                FloatArray(ChamberlainDuquetteArea::chamberlain_duquette_signed_area(
                    &self.0,
                ))
            }
        }
    };
}

impl_alg!(PointArray);
impl_alg!(LineStringArray);
impl_alg!(PolygonArray);
impl_alg!(MultiPointArray);
impl_alg!(MultiLineStringArray);
impl_alg!(MultiPolygonArray);
impl_alg!(GeometryArray);
