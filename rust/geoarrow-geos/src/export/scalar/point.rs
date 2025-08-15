use geo_traits::PointTrait;

use crate::export::scalar::coord::coord_to_geos;

pub(crate) fn to_geos_point(
    point: &impl PointTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    if let Some(coord) = point.coord() {
        let coord_seq = coord_to_geos(&coord)?;
        Ok(geos::Geometry::create_point(coord_seq)?)
    } else {
        Ok(geos::Geometry::create_empty_point()?)
    }
}
