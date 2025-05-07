use geoarrow_schema::{CoordType, Dimension, LineStringType};
use geoarrow_test::raw;
use wkt::types::LineString;
use wkt::wkt;

use crate::array::LineStringArray;
use crate::builder::LineStringBuilder;

pub(crate) fn ls0() -> LineString {
    wkt! { LINESTRING (0. 1., 1. 2.) }
}

pub(crate) fn ls1() -> LineString {
    wkt! { LINESTRING (3. 4., 5. 6.) }
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
