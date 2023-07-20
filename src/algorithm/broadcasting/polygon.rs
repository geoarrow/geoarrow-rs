use arrow2::types::Offset;

use crate::array::polygon::PolygonArrayValuesIter;
use crate::array::PolygonArray;
use crate::scalar::Polygon;

/// An enum over a [`Polygon`] scalar and [`PolygonArray`] array.
///
/// [`IntoIterator`] is implemented for this, where it will iterate over the `Array` variant
/// normally but will iterate over the `Scalar` variant forever.
#[derive(Debug, Clone)]
pub enum BroadcastablePolygon<'a, O: Offset> {
    Scalar(Polygon<'a, O>),
    Array(PolygonArray<O>),
}

pub enum BroadcastPolygonIter<'a, O: Offset> {
    Scalar(Polygon<'a, O>),
    Array(PolygonArrayValuesIter<'a, O>),
}

impl<'a, O: Offset> IntoIterator for &'a BroadcastablePolygon<'a, O> {
    type Item = Polygon<'a, O>;
    type IntoIter = BroadcastPolygonIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastablePolygon::Array(arr) => BroadcastPolygonIter::Array(arr.values_iter()),
            BroadcastablePolygon::Scalar(val) => BroadcastPolygonIter::Scalar(val.clone()),
        }
    }
}

impl<'a, O: Offset> Iterator for BroadcastPolygonIter<'a, O> {
    type Item = Polygon<'a, O>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastPolygonIter::Array(arr) => arr.next(),
            BroadcastPolygonIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
