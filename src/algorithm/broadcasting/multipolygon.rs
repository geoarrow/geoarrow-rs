use arrow2::types::Offset;

use crate::array::multipolygon::MultiPolygonArrayValuesIter;
use crate::array::MultiPolygonArray;

/// An enum over a [`MultiPolygon`][geo::MultiPolygon] scalar and [`MultiPolygonArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableMultiPolygon<O: Offset> {
    Scalar(geo::MultiPolygon),
    Array(MultiPolygonArray<O>),
}

pub enum BroadcastMultiPolygonIter<'a, O: Offset> {
    Scalar(geo::MultiPolygon),
    Array(MultiPolygonArrayValuesIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastableMultiPolygon<O> {
    type Item = geo::MultiPolygon;
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
    type Item = geo::MultiPolygon;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiPolygonIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastMultiPolygonIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
