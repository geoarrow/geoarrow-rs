use geo_traits::GeometryCollectionTrait;

use crate::export::scalar::to_geos_geometry;

pub(crate) fn to_geos_geometry_collection(
    gc: &impl GeometryCollectionTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    geos::Geometry::create_geometry_collection(
        gc.geometries()
            .map(|geometry| to_geos_geometry(&geometry))
            .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
    )
}
