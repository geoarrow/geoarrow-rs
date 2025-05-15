use geo_traits::RectTrait;
use geoarrow_schema::{BoxType, Dimension};
use wkt::types::Coord;

use crate::array::RectArray;
use crate::builder::RectBuilder;

#[derive(PartialEq, Clone, Copy)]
pub struct Rect {
    min: Coord<f64>,
    max: Coord<f64>,
}

impl RectTrait for Rect {
    type T = f64;
    type CoordType<'a> = Coord;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn min(&self) -> Self::CoordType<'_> {
        self.min
    }

    fn max(&self) -> Self::CoordType<'_> {
        self.max
    }
}

pub(crate) fn r0() -> Rect {
    let min = Coord {
        x: 10.,
        y: 20.,
        z: None,
        m: None,
    };
    let max = Coord {
        x: 30.,
        y: 10.,
        z: None,
        m: None,
    };
    Rect { min, max }
}

pub(crate) fn r1() -> Rect {
    let min = Coord {
        x: 100.,
        y: 200.,
        z: None,
        m: None,
    };
    let max = Coord {
        x: 300.,
        y: 100.,
        z: None,
        m: None,
    };
    Rect { min, max }
}

pub(crate) fn r_array() -> RectArray {
    let geoms = [Some(r0()), None, Some(r1()), None];
    let typ = BoxType::new(Dimension::XY, Default::default());
    RectBuilder::from_nullable_rects(geoms.iter().map(|x| x.as_ref()), typ).finish()
}
