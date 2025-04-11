use geo_types::{MultiPolygon, polygon};

use crate::array::MultiPolygonArray;
use crate::builder::MultiPolygonBuilder;
use geoarrow_schema::{CoordType, Dimension, MultiPolygonType};

pub(crate) fn mp0() -> MultiPolygon {
    MultiPolygon::new(vec![
        polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ],
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
        ),
    ])
}

pub(crate) fn mp1() -> MultiPolygon {
    MultiPolygon::new(vec![
        polygon![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ],
        polygon![
            (x: -110., y: 44.),
            (x: -110., y: 42.),
            (x: -105., y: 42.),
            (x: -105., y: 44.),
        ],
    ])
}

pub(crate) fn mp_array(coord_type: CoordType) -> MultiPolygonArray {
    let geoms = vec![Some(mp0()), None, Some(mp1()), None];
    let typ = MultiPolygonType::new(coord_type, Dimension::XY, Default::default());
    MultiPolygonBuilder::from_nullable_multi_polygons(&geoms, typ).finish()
}
