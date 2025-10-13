use geo_traits::PolygonTrait;

use crate::export::scalar::linestring::to_geos_linear_ring;

pub(crate) fn to_geos_polygon(
    polygon: &impl PolygonTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    if let Some(exterior) = polygon.exterior() {
        let exterior = to_geos_linear_ring(&exterior)?;
        let interiors = polygon
            .interiors()
            .map(|interior| to_geos_linear_ring(&interior))
            .collect::<std::result::Result<Vec<_>, geos::Error>>()?;
        geos::Geometry::create_polygon(exterior, interiors)
    } else {
        geos::Geometry::create_empty_polygon()
    }
}
