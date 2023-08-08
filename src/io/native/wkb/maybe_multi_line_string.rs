use crate::algorithm::native::eq::multi_line_string_eq;
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

impl<'a> WKBMaybeMultiLineString<'a> {
    /// Check if this has equal coordinates as some other MultiLineString object
    pub fn equals_multi_line_string(&self, other: impl MultiLineStringTrait<'a, T = f64>) -> bool {
        multi_line_string_eq(self, other)
    }
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::native::wkb::geometry::Endianness;
    use crate::test::linestring::ls0;
    use crate::test::multilinestring::ml0;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn line_string_round_trip() {
        let geom = ls0();
        let buf = geo::Geometry::LineString(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBMaybeMultiLineString::LineString(WKBLineString::new(
            &buf,
            Endianness::LittleEndian,
            0,
        ));

        assert!(wkb_geom.equals_multi_line_string(geo::MultiLineString(vec![geom])));
    }

    #[test]
    fn multi_line_string_round_trip() {
        let geom = ml0();
        let buf = geo::Geometry::MultiLineString(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBMaybeMultiLineString::MultiLineString(WKBMultiLineString::new(
            &buf,
            Endianness::LittleEndian,
        ));

        assert!(wkb_geom.equals_multi_line_string(geom));
    }
}
