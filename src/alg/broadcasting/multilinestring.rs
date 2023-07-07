use crate::array::multilinestring::MultiLineStringArrayValuesIter;
use crate::array::MultiLineStringArray;

pub enum BroadcastableMultiLineString {
    Scalar(geo::MultiLineString),
    Array(MultiLineStringArray),
}

pub enum BroadcastMultiLineStringIter<'a> {
    Scalar(geo::MultiLineString),
    Array(MultiLineStringArrayValuesIter<'a>),
}

impl<'a> IntoIterator for &'a BroadcastableMultiLineString {
    type Item = geo::MultiLineString;
    type IntoIter = BroadcastMultiLineStringIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            BroadcastableMultiLineString::Array(arr) => {
                BroadcastMultiLineStringIter::Array(arr.values_iter())
            }
            BroadcastableMultiLineString::Scalar(val) => {
                BroadcastMultiLineStringIter::Scalar(val.clone())
            }
        }
    }
}

impl<'a> Iterator for BroadcastMultiLineStringIter<'a> {
    type Item = geo::MultiLineString;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BroadcastMultiLineStringIter::Array(arr) => arr.next().map(|item| item.into()),
            BroadcastMultiLineStringIter::Scalar(val) => Some(val.to_owned()),
        }
    }
}
