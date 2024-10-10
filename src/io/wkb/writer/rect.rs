use std::io::Write;

use crate::error::Result;
use crate::geo_traits::RectTrait;
use crate::io::geo::rect_to_geo;
use crate::io::wkb::writer::{polygon_wkb_size, write_polygon_as_wkb};

/// The byte length of a Rect encoded as a WKBPolygon
pub fn rect_wkb_size(geom: &impl RectTrait) -> usize {
    let poly = rect_to_geo(geom).to_polygon();
    polygon_wkb_size(&poly)
}

/// Write a Rect to a Writer encoded as WKB
///
/// This converts the Rect to a Polygon and then encodes that polygon.
pub fn write_rect_as_wkb<W: Write>(writer: W, geom: &impl RectTrait<T = f64>) -> Result<()> {
    // Note, this only works for 2D rects and will lose the third dimension on 3D rects
    let poly = rect_to_geo(geom).to_polygon();
    write_polygon_as_wkb(writer, &poly)
}
