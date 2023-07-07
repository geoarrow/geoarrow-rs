use geo::AffineTransform;
use geoarrow::alg::broadcasting::BroadcastableVec;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct BroadcastableAffine(pub(crate) BroadcastableVec<AffineTransform>);

#[wasm_bindgen]
impl BroadcastableAffine {
    #[wasm_bindgen]
    pub fn from_scalar(transform: &[f64]) -> Self {
        assert_eq!(transform.len(), 6);
        let transform = AffineTransform::new(
            transform[0],
            transform[1],
            transform[2],
            transform[3],
            transform[4],
            transform[5],
        );
        Self(BroadcastableVec::Scalar(transform))
    }

    #[wasm_bindgen]
    pub fn from_array(transform: Vec<f64>) -> Self {
        assert_eq!(
            transform.len() % 6,
            0,
            "array of transforms must be divisible by 6."
        );
        let transforms: Vec<AffineTransform> = transform
            .chunks_exact(6)
            .map(|chunk| {
                AffineTransform::new(chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5])
            })
            .collect();

        Self(BroadcastableVec::Array(transforms))
    }
}
