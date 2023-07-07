use crate::array::multipoint::MultiPointArrayValuesIter;
use crate::array::MultiPointArray;

pub enum BroadcastableMultiPoint {
    Scalar(geo::MultiPoint),
    Array(MultiPointArray),
}

pub enum BroadcastMultiPointIter<'a> {
    Scalar(geo::MultiPoint),
    Array(MultiPointArrayValuesIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastableMultiPoint {
    type Item = geo::MultiPoint;
    type IntoIter = BroadcastMultiPointIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiPoint::Array(arr) => {
                BroadcastMultiPointIter::Array(arr.values_iter())
            }
            BroadcastableMultiPoint::Scalar(val) => BroadcastMultiPointIter::Scalar(val.clone()),
        }
    }
}

impl<'a> Iterator for BroadcastMultiPointIter<'a> {
    type Item = geo::MultiPoint;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiPointIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastMultiPointIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
