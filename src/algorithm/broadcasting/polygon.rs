use crate::array::polygon::PolygonArrayValuesIter;
use crate::array::PolygonArray;

pub enum BroadcastablePolygon {
    Scalar(geo::Polygon),
    Array(PolygonArray),
}

pub enum BroadcastPolygonIter<'a> {
    Scalar(geo::Polygon),
    Array(PolygonArrayValuesIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastablePolygon {
    type Item = geo::Polygon;
    type IntoIter = BroadcastPolygonIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastablePolygon::Array(arr) => BroadcastPolygonIter::Array(arr.values_iter()),
            BroadcastablePolygon::Scalar(val) => BroadcastPolygonIter::Scalar(val.clone()),
        }
    }
}

impl<'a> Iterator for BroadcastPolygonIter<'a> {
    type Item = geo::Polygon;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastPolygonIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastPolygonIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
