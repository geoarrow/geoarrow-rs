use arrow2::types::Offset;

use crate::array::multilinestring::MultiLineStringArrayValuesIter;
use crate::array::MultiLineStringArray;

/// An enum over a [`MultiLineString`][geo::MultiLineString] scalar and [`MultiLineStringArray`]
/// array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableMultiLineString<O: Offset> {
    Scalar(geo::MultiLineString),
    Array(MultiLineStringArray<O>),
}

pub enum BroadcastMultiLineStringIter<'a, O: Offset> {
    Scalar(geo::MultiLineString),
    Array(MultiLineStringArrayValuesIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastableMultiLineString<O> {
    type Item = geo::MultiLineString;
    type IntoIter = BroadcastMultiLineStringIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiLineString::Array(arr) => {
                BroadcastMultiLineStringIter::Array(arr.values_iter())
            }
            BroadcastableMultiLineString::Scalar(val) => {
                BroadcastMultiLineStringIter::Scalar(val.clone())
            }
        }
    }
}

impl<'a, O: Offset> Iterator for BroadcastMultiLineStringIter<'a, O> {
    type Item = geo::MultiLineString;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiLineStringIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastMultiLineStringIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
