use arrow_array::OffsetSizeTrait;

use crate::array::multipolygon::MultiPolygonArrayIter;
use crate::array::MultiPolygonArray;
use crate::scalar::MultiPolygon;

/// An enum over a [`MultiPolygon`] scalar and [`MultiPolygonArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableMultiPolygon<'a, O: OffsetSizeTrait> {
    Scalar(MultiPolygon<'a, O>),
    Array(MultiPolygonArray<O>),
}

pub enum BroadcastMultiPolygonIter<'a, O: OffsetSizeTrait> {
    Scalar(MultiPolygon<'a, O>),
    Array(MultiPolygonArrayIter<'a, O>),
}

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a BroadcastableMultiPolygon<'a, O> {
    type Item = Option<MultiPolygon<'a, O>>;
    type IntoIter = BroadcastMultiPolygonIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiPolygon::Array(arr) => {
                BroadcastMultiPolygonIter::Array(MultiPolygonArrayIter::new(arr))
            }
            BroadcastableMultiPolygon::Scalar(val) => {
                BroadcastMultiPolygonIter::Scalar(val.clone())
            }
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for BroadcastMultiPolygonIter<'a, O> {
    type Item = Option<MultiPolygon<'a, O>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiPolygonIter::Array(arr) => arr.next(),
            BroadcastMultiPolygonIter::Scalar(val) => Some(Some(val.to_owned())),
        }
    }
}
