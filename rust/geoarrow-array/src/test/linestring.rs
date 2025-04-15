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

macro_rules! impl_mod {
    ($mod_name:ident, $dim:expr) => {
        pub mod $mod_name {
            use super::*;

            pub fn array(coord_type: CoordType) -> LineStringArray {
                let typ = LineStringType::new(coord_type, $dim, Default::default());
                LineStringBuilder::from_nullable_line_strings(
                    &raw::linestring::$mod_name::geoms(),
                    typ,
                )
                .finish()
            }
        }
    };
}

impl_mod!(xy, Dimension::XY);
impl_mod!(xyz, Dimension::XYZ);
impl_mod!(xym, Dimension::XYM);
impl_mod!(xyzm, Dimension::XYZM);
