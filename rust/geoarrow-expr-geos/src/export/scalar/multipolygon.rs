use geo_traits::MultiPolygonTrait;

use crate::export::scalar::polygon::to_geos_polygon;

pub(crate) fn to_geos_multi_polygon(
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    geos::Geometry::create_multipolygon(
        multi_polygon
            .polygons()
            .map(|polygon| to_geos_polygon(&polygon))
            .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
    )
}
