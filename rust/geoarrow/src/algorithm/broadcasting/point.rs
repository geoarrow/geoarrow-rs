use crate::array::PointArray;
use crate::scalar::Point;

/// An enum over a [`Point`] scalar and [`PointArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug)]
pub enum BroadcastablePoint<'a> {
    Scalar(Point<'a>),
    Array(PointArray),
}

pub enum BroadcastPointIter<'a> {
    Scalar(Point<'a>),
    Array(PointArrayIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastablePoint<'a> {
    type Item = Option<Point<'a>>;
    type IntoIter = BroadcastPointIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastablePoint::Array(arr) => BroadcastPointIter::Array(PointArrayIter::new(arr)),
            BroadcastablePoint::Scalar(val) => BroadcastPointIter::Scalar(val.to_owned()),
        }
    }
}

impl<'a> Iterator for BroadcastPointIter<'a> {
    type Item = Option<Point<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastPointIter::Array(arr) => arr.next(),
            BroadcastPointIter::Scalar(val) => Some(Some(val.to_owned())),
        }
    }
}
