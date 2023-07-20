use arrow2::types::Offset;

use crate::array::multipolygon::MultiPolygonArrayValuesIter;
use crate::array::MultiPolygonArray;
use crate::scalar::MultiPolygon;

/// An enum over a [`MultiPolygon`] scalar and [`MultiPolygonArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableMultiPolygon<'a, O: Offset> {
    Scalar(MultiPolygon<'a, O>),
    Array(MultiPolygonArray<O>),
}

pub enum BroadcastMultiPolygonIter<'a, O: Offset> {
    Scalar(MultiPolygon<'a, O>),
    Array(MultiPolygonArrayValuesIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastableMultiPolygon<'a, O> {
    type Item = MultiPolygon<'a, O>;
    type IntoIter = BroadcastMultiPolygonIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiPolygon::Array(arr) => {
                BroadcastMultiPolygonIter::Array(arr.values_iter())
            }
            BroadcastableMultiPolygon::Scalar(val) => {
                BroadcastMultiPolygonIter::Scalar(val.clone())
            }
        }
    }
}

impl<'a, O: Offset> Iterator for BroadcastMultiPolygonIter<'a, O> {
    type Item = MultiPolygon<'a, O>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiPolygonIter::Array(arr) => arr.next(),
            BroadcastMultiPolygonIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
