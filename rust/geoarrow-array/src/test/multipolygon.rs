use geo_types::{MultiPolygon, polygon};
use geoarrow_schema::{CoordType, Dimension, MultiPolygonType};
use geoarrow_test::raw;

use crate::array::MultiPolygonArray;
use crate::builder::MultiPolygonBuilder;

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
    let typ = MultiPolygonType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
    MultiPolygonBuilder::from_nullable_multi_polygons(&geoms, typ).finish()
}

pub fn array(coord_type: CoordType, dim: Dimension) -> MultiPolygonArray {
    let typ = MultiPolygonType::new(dim, Default::default()).with_coord_type(coord_type);
    let geoms = match dim {
        Dimension::XY => raw::multipolygon::xy::geoms(),
        Dimension::XYZ => raw::multipolygon::xyz::geoms(),
        Dimension::XYM => raw::multipolygon::xym::geoms(),
        Dimension::XYZM => raw::multipolygon::xyzm::geoms(),
    };
    MultiPolygonBuilder::from_nullable_multi_polygons(&geoms, typ).finish()
}
