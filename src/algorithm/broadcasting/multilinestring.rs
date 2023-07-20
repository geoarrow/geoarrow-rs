use arrow2::types::Offset;

use crate::array::multilinestring::MultiLineStringArrayValuesIter;
use crate::array::MultiLineStringArray;
use crate::scalar::MultiLineString;

/// An enum over a [`MultiLineString`] scalar and [`MultiLineStringArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableMultiLineString<'a, O: Offset> {
    Scalar(MultiLineString<'a, O>),
    Array(MultiLineStringArray<O>),
}

pub enum BroadcastMultiLineStringIter<'a, O: Offset> {
    Scalar(MultiLineString<'a, O>),
    Array(MultiLineStringArrayValuesIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastableMultiLineString<'a, O> {
    type Item = MultiLineString<'a, O>;
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
    type Item = MultiLineString<'a, O>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiLineStringIter::Array(arr) => arr.next(),
            BroadcastMultiLineStringIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
