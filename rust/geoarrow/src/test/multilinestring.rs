use geo::{line_string, MultiLineString};

use crate::array::{MultiLineStringArray, MultiLineStringBuilder};
use geoarrow_schema::Dimension;

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

pub(crate) fn ml_array() -> MultiLineStringArray {
    let geoms = vec![ml0(), ml1()];
    MultiLineStringBuilder::from_multi_line_strings(
        &geoms,
        Dimension::XY,
        Default::default(),
        Default::default(),
    )
    .finish()
}
