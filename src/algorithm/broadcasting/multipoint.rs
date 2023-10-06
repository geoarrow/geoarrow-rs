use arrow_array::OffsetSizeTrait;

use crate::array::multipoint::MultiPointArrayValuesIter;
use crate::array::MultiPointArray;
use crate::scalar::MultiPoint;

/// An enum over a [`MultiPoint`] scalar and [`MultiPointArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastableMultiPoint<'a, O: OffsetSizeTrait> {
    Scalar(MultiPoint<'a, O>),
    Array(MultiPointArray<O>),
}

pub enum BroadcastMultiPointIter<'a, O: OffsetSizeTrait> {
    Scalar(MultiPoint<'a, O>),
    Array(MultiPointArrayValuesIter<'a, O>),
}

impl<'a, O: OffsetSizeTrait> IntoIterator for &'a BroadcastableMultiPoint<'a, O> {
    type Item = MultiPoint<'a, O>;
    type IntoIter = BroadcastMultiPointIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiPoint::Array(arr) => {
                BroadcastMultiPointIter::Array(arr.values_iter())
            }
            BroadcastableMultiPoint::Scalar(val) => BroadcastMultiPointIter::Scalar(val.clone()),
        }
    }
}

impl<'a, O: OffsetSizeTrait> Iterator for BroadcastMultiPointIter<'a, O> {
    type Item = MultiPoint<'a, O>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiPointIter::Array(arr) => arr.next(),
            BroadcastMultiPointIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
