use crate::array::{PolygonArray, PolygonBuilder};
use crate::datatypes::Dimension;
use geo::{polygon, Polygon};

pub(crate) fn p0() -> Polygon {
    polygon![
        (x: -111., y: 45.),
        (x: -111., y: 41.),
        (x: -104., y: 41.),
        (x: -104., y: 45.),
    ]
}

pub(crate) fn p1() -> Polygon {
    polygon!(
        exterior: [
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ],
        interiors: [
            [
                (x: -110., y: 44.),
                (x: -110., y: 42.),
                (x: -105., y: 42.),
                (x: -105., y: 44.),
            ],
        ],
    )
}

pub(crate) fn p_array() -> PolygonArray {
    let geoms = vec![p0(), p1()];
    PolygonBuilder::from_polygons(
        &geoms,
        Dimension::XY,
        Default::default(),
        Default::default(),
    )
    .finish()
}
