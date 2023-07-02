use geo::{line_string, MultiLineString};
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
