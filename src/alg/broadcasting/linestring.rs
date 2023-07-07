use crate::array::linestring::LineStringArrayValuesIter;
use crate::array::LineStringArray;

pub enum BroadcastableLineString {
    Scalar(geo::LineString),
    Array(LineStringArray),
}

pub enum BroadcastLineStringIter<'a> {
    Scalar(geo::LineString),
    Array(LineStringArrayValuesIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastableLineString {
    type Item = geo::LineString;
    type IntoIter = BroadcastLineStringIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableLineString::Array(arr) => {
                BroadcastLineStringIter::Array(arr.values_iter())
            }
            BroadcastableLineString::Scalar(val) => BroadcastLineStringIter::Scalar(val.clone()),
        }
    }
}

impl<'a> Iterator for BroadcastLineStringIter<'a> {
    type Item = geo::LineString;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastLineStringIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastLineStringIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
