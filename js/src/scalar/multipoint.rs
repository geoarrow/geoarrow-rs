use geoarrow::scalar::OwnedMultiPoint;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPoint(pub(crate) OwnedMultiPoint<i32>);

impl<'a> From<MultiPoint> for geoarrow::scalar::MultiPoint<'a, i32> {
    fn from(value: MultiPoint) -> Self {
        value.0.into()
    }
}

impl From<MultiPoint> for geoarrow::scalar::OwnedMultiPoint<i32> {
    fn from(value: MultiPoint) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::MultiPoint<'a, i32>> for MultiPoint {
    fn from(value: geoarrow::scalar::MultiPoint<'a, i32>) -> Self {
        MultiPoint(value.into())
    }
}
