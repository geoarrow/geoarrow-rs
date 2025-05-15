use geoarrow_schema::{CoordType, Dimension, MultiLineStringType};
use geoarrow_test::raw;
use wkt::types::MultiLineString;
use wkt::wkt;

use crate::array::MultiLineStringArray;
use crate::builder::MultiLineStringBuilder;

pub(crate) fn ml0() -> MultiLineString {
    wkt! { MULTILINESTRING ((-111. 45., -111. 41., -104. 41., -104. 45.)) }
}

pub(crate) fn ml1() -> MultiLineString {
    wkt! { MULTILINESTRING ((-111. 45., -111. 41., -104. 41., -104. 45.), (-110. 44., -110. 42., -105. 42., -105. 44.)) }
}

pub(crate) fn ml_array(coord_type: CoordType) -> MultiLineStringArray {
    let geoms = vec![Some(ml0()), None, Some(ml1()), None];
    let typ = MultiLineStringType::new(coord_type, Dimension::XY, Default::default());
    MultiLineStringBuilder::from_nullable_multi_line_strings(&geoms, typ).finish()
}

pub fn array(coord_type: CoordType, dim: Dimension) -> MultiLineStringArray {
    let typ = MultiLineStringType::new(coord_type, dim, Default::default());
    let geoms = match dim {
        Dimension::XY => raw::multilinestring::xy::geoms(),
        Dimension::XYZ => raw::multilinestring::xyz::geoms(),
        Dimension::XYM => raw::multilinestring::xym::geoms(),
        Dimension::XYZM => raw::multilinestring::xyzm::geoms(),
    };
    MultiLineStringBuilder::from_nullable_multi_line_strings(&geoms, typ).finish()
}
