use geo_traits::LineStringTrait;

use crate::export::scalar::coord::{coords_to_geos, dims_to_geos};

pub(crate) fn to_geos_line_string(
    line_string: &impl LineStringTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    let dims = dims_to_geos(line_string.dim());
    let coord_seq = coords_to_geos(line_string.coords(), dims)?;
    geos::Geometry::create_line_string(coord_seq)
}

pub(crate) fn to_geos_linear_ring(
    line_string: &impl LineStringTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    let dims = dims_to_geos(line_string.dim());
    let coord_seq = coords_to_geos(line_string.coords(), dims)?;
    geos::Geometry::create_linear_ring(coord_seq)
}
