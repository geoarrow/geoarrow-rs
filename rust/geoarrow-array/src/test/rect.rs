use geo_types::{Rect, coord};

use crate::array::RectArray;
use crate::builder::RectBuilder;
use geoarrow_schema::{BoxType, Dimension};

pub(crate) fn r0() -> Rect {
    Rect::new(coord! { x: 10., y: 20. }, coord! { x: 30., y: 10. })
}

pub(crate) fn r1() -> Rect {
    Rect::new(coord! { x: 100., y: 200. }, coord! { x: 300., y: 100. })
}

pub(crate) fn r_array() -> RectArray {
    let geoms = [Some(r0()), None, Some(r1()), None];
    let typ = BoxType::new(Dimension::XY, Default::default());
    RectBuilder::from_nullable_rects(geoms.iter().map(|x| x.as_ref()), typ).finish()
}
