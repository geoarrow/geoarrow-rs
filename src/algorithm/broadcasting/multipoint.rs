use arrow2::types::Offset;

use crate::array::multipoint::MultiPointArrayValuesIter;
use crate::array::MultiPointArray;

pub enum BroadcastableMultiPoint<O: Offset> {
    Scalar(geo::MultiPoint),
    Array(MultiPointArray<O>),
}

pub enum BroadcastMultiPointIter<'a, O: Offset> {
    Scalar(geo::MultiPoint),
    Array(MultiPointArrayValuesIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastableMultiPoint<O> {
    type Item = geo::MultiPoint;
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

impl<'a, O: Offset> Iterator for BroadcastMultiPointIter<'a, O> {
    type Item = geo::MultiPoint;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiPointIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastMultiPointIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
