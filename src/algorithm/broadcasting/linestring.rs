use arrow_array::OffsetSizeTrait;

use crate::array::linestring::LineStringArrayIter;
use crate::array::LineStringArray;
use crate::scalar::LineString;

/// An enum over a [`LineString`] scalar and [`LineStringArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableLineString<'a, O: OffsetSizeTrait> {
    Scalar(LineString<'a, O>),
    Array(LineStringArray<O>),
}

pub enum BroadcastLineStringIter<'a, O: OffsetSizeTrait> {
    Scalar(LineString<'a, O>),
    Array(LineStringArrayIter<'a, O>),
}

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a BroadcastableLineString<'a, O> {
    type Item = Option<LineString<'a, O>>;
    type IntoIter = BroadcastLineStringIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableLineString::Array(arr) => {
                BroadcastLineStringIter::Array(LineStringArrayIter::new(arr))
            }
            BroadcastableLineString::Scalar(val) => BroadcastLineStringIter::Scalar(val.clone()),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for BroadcastLineStringIter<'a, O> {
    type Item = Option<LineString<'a, O>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastLineStringIter::Array(arr) => arr.next(),
            BroadcastLineStringIter::Scalar(val) => Some(Some(val.to_owned())),
        }
    }
}
