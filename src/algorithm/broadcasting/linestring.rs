use arrow2::types::Offset;

use crate::array::linestring::LineStringArrayValuesIter;
use crate::array::LineStringArray;

/// An enum over a [`LineString`][geo::LineString] scalar and [`LineStringArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableLineString<O: Offset> {
    Scalar(geo::LineString),
    Array(LineStringArray<O>),
}

pub enum BroadcastLineStringIter<'a, O: Offset> {
    Scalar(geo::LineString),
    Array(LineStringArrayValuesIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastableLineString<O> {
    type Item = geo::LineString;
    type IntoIter = BroadcastLineStringIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableLineString::Array(arr) => {
                BroadcastLineStringIter::Array(arr.values_iter())
            }
            BroadcastableLineString::Scalar(val) => BroadcastLineStringIter::Scalar(val.clone()),
        }
    }
}

impl<'a, O: Offset> Iterator for BroadcastLineStringIter<'a, O> {
    type Item = geo::LineString;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastLineStringIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastLineStringIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
