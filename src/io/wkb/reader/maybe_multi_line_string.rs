use crate::algorithm::native::eq::multi_line_string_eq;
use crate::geo_traits::MultiLineStringTrait;
use crate::io::wkb::reader::linestring::WKBLineString;
use crate::io::wkb::reader::multilinestring::WKBMultiLineString;

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
    pub fn equals_multi_line_string(&self, other: &impl MultiLineStringTrait<T = f64>) -> bool {
        multi_line_string_eq(self, other)
    }
}

impl<'a> MultiLineStringTrait for WKBMaybeMultiLineString<'a> {
    type T = f64;
    type LineStringtype<'b> = WKBLineString<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.dim(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.dim(),
        }
    }

    fn num_line_strings(&self) -> usize {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.num_line_strings(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.num_line_strings(),
        }
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringtype<'_> {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.line_string_unchecked(i),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.line_string_unchecked(i),
        }
    }
}

impl<'a> MultiLineStringTrait for &'a WKBMaybeMultiLineString<'a> {
    type T = f64;
    type LineStringtype<'b> = WKBLineString<'a> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimensions {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.dim(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.dim(),
        }
    }

    fn num_line_strings(&self) -> usize {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.num_line_strings(),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.num_line_strings(),
        }
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringtype<'_> {
        match self {
            WKBMaybeMultiLineString::LineString(geom) => geom.line_string_unchecked(i),
            WKBMaybeMultiLineString::MultiLineString(geom) => geom.line_string_unchecked(i),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::datatypes::Dimension;
    use crate::io::wkb::reader::geometry::Endianness;
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
            Dimension::XY,
        ));

        assert!(wkb_geom.equals_multi_line_string(&geo::MultiLineString(vec![geom])));
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
            Dimension::XY,
        ));

        assert!(wkb_geom.equals_multi_line_string(&geom));
    }
}
