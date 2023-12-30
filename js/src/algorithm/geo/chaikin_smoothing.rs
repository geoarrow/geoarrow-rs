use crate::data::*;
use crate::vector::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_chaikin_smoothing {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using Chaikins algorithm.
            ///
            /// [Chaikins smoothing algorithm](http://www.idav.ucdavis.edu/education/CAGDNotes/Chaikins-Algorithm/Chaikins-Algorithm.html)
            ///
            /// Each iteration of the smoothing doubles the number of vertices of the geometry, so in some
            /// cases it may make sense to apply a simplification afterwards to remove insignificant
            /// coordinates.
            ///
            /// This implementation preserves the start and end vertices of an open linestring and
            /// smoothes the corner between start and end of a closed linestring.
            #[wasm_bindgen(js_name = chaikinSmoothing)]
            pub fn chaikin_smoothing(&self, n_iterations: u32) -> Self {
                use geoarrow::algorithm::geo::ChaikinSmoothing;
                ChaikinSmoothing::chaikin_smoothing(&self.0, n_iterations).into()
            }
        }
    };
}

impl_chaikin_smoothing!(LineStringData);
impl_chaikin_smoothing!(PolygonData);
impl_chaikin_smoothing!(MultiLineStringData);
impl_chaikin_smoothing!(MultiPolygonData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Smoothen `LineString`, `Polygon`, `MultiLineString` and `MultiPolygon` using Chaikins algorithm.
            ///
            /// [Chaikins smoothing algorithm](http://www.idav.ucdavis.edu/education/CAGDNotes/Chaikins-Algorithm/Chaikins-Algorithm.html)
            ///
            /// Each iteration of the smoothing doubles the number of vertices of the geometry, so in some
            /// cases it may make sense to apply a simplification afterwards to remove insignificant
            /// coordinates.
            ///
            /// This implementation preserves the start and end vertices of an open linestring and
            /// smoothes the corner between start and end of a closed linestring.
            #[wasm_bindgen(js_name = chaikinSmoothing)]
            pub fn chaikin_smoothing(&self, n_iterations: u32) -> Self {
                use geoarrow::algorithm::geo::ChaikinSmoothing;
                ChaikinSmoothing::chaikin_smoothing(&self.0, n_iterations).into()
            }
        }
    };
}

impl_vector!(LineStringVector);
impl_vector!(PolygonVector);
impl_vector!(MultiLineStringVector);
impl_vector!(MultiPolygonVector);
