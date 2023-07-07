use crate::array::multipolygon::MultiPolygonArrayValuesIter;
use crate::array::MultiPolygonArray;

pub enum BroadcastableMultiPolygon {
    Scalar(geo::MultiPolygon),
    Array(MultiPolygonArray),
}

pub enum BroadcastMultiPolygonIter<'a> {
    Scalar(geo::MultiPolygon),
    Array(MultiPolygonArrayValuesIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastableMultiPolygon {
    type Item = geo::MultiPolygon;
    type IntoIter = BroadcastMultiPolygonIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiPolygon::Array(arr) => {
                BroadcastMultiPolygonIter::Array(arr.values_iter())
            }
            BroadcastableMultiPolygon::Scalar(val) => {
                BroadcastMultiPolygonIter::Scalar(val.clone())
            }
        }
    }
}

impl<'a> Iterator for BroadcastMultiPolygonIter<'a> {
    type Item = geo::MultiPolygon;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiPolygonIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastMultiPolygonIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
