use geo_traits::MultiPointTrait;

use crate::export::scalar::point::to_geos_point;

pub(crate) fn to_geos_multi_point(
    multi_point: &impl MultiPointTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    geos::Geometry::create_multipoint(
        multi_point
            .points()
            .map(|point| to_geos_point(&point))
            .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
    )
}
