use geo::{line_string, LineString};

use crate::array::LineStringArray;

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

pub(crate) fn ls_array() -> LineStringArray<i32> {
    vec![ls0(), ls1()].as_slice().into()
}
