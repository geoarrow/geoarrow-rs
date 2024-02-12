use crate::scalar::WKB;
use arrow_array::{GenericBinaryArray, OffsetSizeTrait};

#[derive(Debug, PartialEq)]
pub struct OwnedWKB<O: OffsetSizeTrait> {
    arr: GenericBinaryArray<O>,
    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedWKB<O> {
    pub fn new(arr: GenericBinaryArray<O>, geom_index: usize) -> Self {
        Self { arr, geom_index }
    }
}

impl<'a, O: OffsetSizeTrait> From<OwnedWKB<O>> for WKB<'a, O> {
    fn from(value: OwnedWKB<O>) -> Self {
        Self::new_owned(value.arr, value.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedWKB<O>> for WKB<'a, O> {
    fn from(value: &'a OwnedWKB<O>) -> Self {
        Self::new_borrowed(&value.arr, value.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> From<WKB<'a, O>> for OwnedWKB<O> {
    fn from(value: WKB<'a, O>) -> Self {
        let (arr, geom_index) = value.into_owned_inner();
        Self::new(arr, geom_index)
    }
}
