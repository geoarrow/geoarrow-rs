use geo_traits::GeometryTrait;

use crate::export::scalar::geometrycollection::to_geos_geometry_collection;
use crate::export::scalar::linestring::to_geos_line_string;
use crate::export::scalar::multilinestring::to_geos_multi_line_string;
use crate::export::scalar::multipoint::to_geos_multi_point;
use crate::export::scalar::multipolygon::to_geos_multi_polygon;
use crate::export::scalar::point::to_geos_point;
use crate::export::scalar::polygon::to_geos_polygon;

/// Converts a geometry into a GEOS geometry.
///
/// # Errors
///
/// `Rect`, `Triangle`, and `Line` are unsupported by GEOS,
/// and providing one as input will fail.
pub fn to_geos_geometry(
    geometry: &impl GeometryTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    use geo_traits::GeometryType::*;

    match geometry.as_type() {
        Point(g) => to_geos_point(g),
        LineString(g) => to_geos_line_string(g),
        Polygon(g) => to_geos_polygon(g),
        MultiPoint(g) => to_geos_multi_point(g),
        MultiLineString(g) => to_geos_multi_line_string(g),
        MultiPolygon(g) => to_geos_multi_polygon(g),
        GeometryCollection(g) => to_geos_geometry_collection(g),
        Rect(_) => Err(geos::Error::ConversionError(
            "unsupported rect in conversion to GEOS".to_string(),
        )),
        Triangle(_) => Err(geos::Error::ConversionError(
            "unsupported Triangle in conversion to GEOS".to_string(),
        )),
        Line(_) => Err(geos::Error::ConversionError(
            "unsupported Line in conversion to GEOS".to_string(),
        )),
    }
}
