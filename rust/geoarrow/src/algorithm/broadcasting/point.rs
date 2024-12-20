use geo_traits::to_geo::ToGeoPoint;
use geo_traits::PointTrait;

use crate::algorithm::broadcasting::iterator::PointArrayIter;
use crate::array::PointArray;
use crate::trait_::NativeScalar;

/// An enum over a [`Point`] scalar and [`PointArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug)]
pub enum BroadcastablePoint {
    Scalar(geo::Point),
    Array(PointArray),
}

impl<G: PointTrait<T = f64>> From<G> for BroadcastablePoint {
    fn from(value: G) -> Self {
        Self::Scalar(value.to_point())
    }
}

impl From<PointArray> for BroadcastablePoint {
    fn from(value: PointArray) -> Self {
        Self::Array(value)
    }
}

pub enum BroadcastPointIter<'a> {
    Scalar(geo::Point),
    Array(PointArrayIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastablePoint {
    type Item = Option<geo::Point>;
    type IntoIter = BroadcastPointIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastablePoint::Array(arr) => BroadcastPointIter::Array(PointArrayIter::new(arr)),
            BroadcastablePoint::Scalar(val) => BroadcastPointIter::Scalar(val.to_owned()),
        }
    }
}

impl Iterator for BroadcastPointIter<'_> {
    type Item = Option<geo::Point>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastPointIter::Array(arr) => arr.next().map(|x| x.map(|y| y.to_geo())),
            BroadcastPointIter::Scalar(val) => Some(Some(val.to_owned())),
        }
    }
}
