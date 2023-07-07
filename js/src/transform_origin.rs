use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct TransformOrigin(pub(crate) geoarrow::alg::geo::TransformOrigin);

#[wasm_bindgen]
impl TransformOrigin {
    #[wasm_bindgen]
    pub fn centroid() -> Self {
        Self(geoarrow::alg::geo::TransformOrigin::Centroid)
    }

    #[wasm_bindgen]
    pub fn center() -> Self {
        Self(geoarrow::alg::geo::TransformOrigin::Center)
    }

    #[wasm_bindgen]
    pub fn point(x: f64, y: f64) -> Self {
        Self(geoarrow::alg::geo::TransformOrigin::Point(geo::Point::new(
            x, y,
        )))
    }
}
