use geo_types::{MultiLineString, line_string};
use geoarrow_schema::{CoordType, Dimension, MultiLineStringType};

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
    let typ = MultiLineStringType::new(coord_type, Dimension::XY, Default::default());
    MultiLineStringBuilder::from_nullable_multi_line_strings(&geoms, typ).finish()
}
