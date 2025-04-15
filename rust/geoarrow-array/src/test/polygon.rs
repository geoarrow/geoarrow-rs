use geo_types::{Polygon, polygon};
use geoarrow_schema::{CoordType, Dimension, PolygonType};
use geoarrow_test::raw;

use crate::array::PolygonArray;
use crate::builder::PolygonBuilder;

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

pub(crate) fn p_array(coord_type: CoordType) -> PolygonArray {
    let geoms = vec![Some(p0()), None, Some(p1()), None];
    let typ = PolygonType::new(coord_type, Dimension::XY, Default::default());
    PolygonBuilder::from_nullable_polygons(&geoms, typ).finish()
}

pub fn array(coord_type: CoordType, dim: Dimension) -> PolygonArray {
    let typ = PolygonType::new(coord_type, dim, Default::default());
    let geoms = match dim {
        Dimension::XY => raw::polygon::xy::geoms(),
        Dimension::XYZ => raw::polygon::xyz::geoms(),
        Dimension::XYM => raw::polygon::xym::geoms(),
        Dimension::XYZM => raw::polygon::xyzm::geoms(),
    };
    PolygonBuilder::from_nullable_polygons(&geoms, typ).finish()
}
