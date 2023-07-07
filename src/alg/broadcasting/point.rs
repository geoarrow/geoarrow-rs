use crate::array::point::PointArrayValuesIter;
use crate::array::PointArray;

pub enum BroadcastablePoint {
    Scalar(geo::Point),
    Array(PointArray),
}

pub enum BroadcastPointIter<'a> {
    Scalar(geo::Point),
    Array(PointArrayValuesIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastablePoint {
    type Item = geo::Point;
    type IntoIter = BroadcastPointIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastablePoint::Array(arr) => BroadcastPointIter::Array(arr.values_iter()),
            BroadcastablePoint::Scalar(val) => BroadcastPointIter::Scalar(*val),
        }
    }
}

impl<'a> Iterator for BroadcastPointIter<'a> {
    type Item = geo::Point;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastPointIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastPointIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
