use geo_types::{LineString, line_string};
use geoarrow_schema::{CoordType, Dimension, LineStringType};
use geoarrow_test::raw;

use crate::array::LineStringArray;
use crate::builder::LineStringBuilder;

pub(crate) fn ls0() -> LineString {
    line_string![
        (x: 0., y: 1.),
        (x: 1., y: 2.)
    ]
}

pub(crate) fn ls1() -> LineString {
    line_string![
        (x: 3., y: 4.),
        (x: 5., y: 6.)
    ]
}

pub(crate) fn ls_array(coord_type: CoordType) -> LineStringArray {
    let geoms = vec![Some(ls0()), None, Some(ls1()), None];
    let typ = LineStringType::new(coord_type, Dimension::XY, Default::default());
    LineStringBuilder::from_nullable_line_strings(&geoms, typ).finish()
}

pub fn array(coord_type: CoordType, dim: Dimension) -> LineStringArray {
    let typ = LineStringType::new(coord_type, dim, Default::default());
    let geoms = match dim {
        Dimension::XY => raw::linestring::xy::geoms(),
        Dimension::XYZ => raw::linestring::xyz::geoms(),
        Dimension::XYM => raw::linestring::xym::geoms(),
        Dimension::XYZM => raw::linestring::xyzm::geoms(),
    };
    LineStringBuilder::from_nullable_line_strings(&geoms, typ).finish()
}
