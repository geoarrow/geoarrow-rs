use arrow_array::OffsetSizeTrait;

use crate::array::multilinestring::MultiLineStringArrayIter;
use crate::array::MultiLineStringArray;
use crate::scalar::MultiLineString;

/// An enum over a [`MultiLineString`] scalar and [`MultiLineStringArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableMultiLineString<'a, O: OffsetSizeTrait> {
    Scalar(MultiLineString<'a, O>),
    Array(MultiLineStringArray<O>),
}

pub enum BroadcastMultiLineStringIter<'a, O: OffsetSizeTrait> {
    Scalar(MultiLineString<'a, O>),
    Array(MultiLineStringArrayIter<'a, O>),
}

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a BroadcastableMultiLineString<'a, O> {
    type Item = Option<MultiLineString<'a, O>>;
    type IntoIter = BroadcastMultiLineStringIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiLineString::Array(arr) => {
                BroadcastMultiLineStringIter::Array(MultiLineStringArrayIter::new(arr))
            }
            BroadcastableMultiLineString::Scalar(val) => {
                BroadcastMultiLineStringIter::Scalar(val.clone())
            }
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for BroadcastMultiLineStringIter<'a, O> {
    type Item = Option<MultiLineString<'a, O>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiLineStringIter::Array(arr) => arr.next(),
            BroadcastMultiLineStringIter::Scalar(val) => Some(Some(val.to_owned())),
        }
    }
}
