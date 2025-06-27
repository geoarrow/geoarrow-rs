use geo_types::{MultiLineString, line_string};
use geoarrow_schema::{CoordType, Dimension, MultiLineStringType};
use geoarrow_test::raw;

use crate::array::MultiLineStringArray;
use crate::builder::MultiLineStringBuilder;

pub(crate) fn ml0() -> MultiLineString {
    MultiLineString::new(vec![line_string![
        (x: -111., y: 45.),
        (x: -111., y: 41.),
        (x: -104., y: 41.),
        (x: -104., y: 45.),
    ]])
}

pub(crate) fn ml1() -> MultiLineString {
    MultiLineString::new(vec![
        line_string![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ],
        line_string![
            (x: -110., y: 44.),
            (x: -110., y: 42.),
            (x: -105., y: 42.),
            (x: -105., y: 44.),
        ],
    ])
}

pub(crate) fn ml_array(coord_type: CoordType) -> MultiLineStringArray {
    let geoms = vec![Some(ml0()), None, Some(ml1()), None];
    let typ =
        MultiLineStringType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
    MultiLineStringBuilder::from_nullable_multi_line_strings(&geoms, typ).finish()
}

pub fn array(coord_type: CoordType, dim: Dimension) -> MultiLineStringArray {
    let typ = MultiLineStringType::new(dim, Default::default()).with_coord_type(coord_type);
    let geoms = match dim {
        Dimension::XY => raw::multilinestring::xy::geoms(),
        Dimension::XYZ => raw::multilinestring::xyz::geoms(),
        Dimension::XYM => raw::multilinestring::xym::geoms(),
        Dimension::XYZM => raw::multilinestring::xyzm::geoms(),
    };
    MultiLineStringBuilder::from_nullable_multi_line_strings(&geoms, typ).finish()
}
