use crate::geo_traits::MultiLineStringTrait;
use crate::io::native::wkb::linestring::WKBLineString;
use crate::io::native::wkb::multilinestring::WKBMultiLineString;
use std::iter::Cloned;
use std::slice::Iter;

/// An WKB object that can be either a WKBLineString or a WKBMultiLineString.
///
/// This is used for casting a mix of linestrings and multi linestrings to an array of multi linestrings
#[derive(Debug, Clone)]
pub enum WKBMaybeMultiLineString<'a> {
    LineString(WKBLineString<'a>),
    MultiLineString(WKBMultiLineString<'a>),
}

impl<'a> MultiLineStringTrait<'a> for WKBMaybeMultiLineString<'a> {
    type T = f64;
    type ItemType = WKBLineString<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_lines(&self) -> usize {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.num_lines(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.num_lines(),
        }
    }

    fn line(&self, i: usize) -> Option<Self::ItemType> {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.line(i),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.line(i),
        }
    }

    fn lines(&'a self) -> Self::Iter {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.lines(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.lines(),
        }
    }
}

impl<'a> MultiLineStringTrait<'a> for &WKBMaybeMultiLineString<'a> {
    type T = f64;
    type ItemType = WKBLineString<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_lines(&self) -> usize {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.num_lines(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.num_lines(),
        }
    }

    fn line(&self, i: usize) -> Option<Self::ItemType> {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.line(i),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.line(i),
        }
    }

    fn lines(&'a self) -> Self::Iter {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.lines(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.lines(),
        }
    }
}
