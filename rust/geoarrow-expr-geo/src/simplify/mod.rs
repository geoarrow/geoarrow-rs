mod shared;
mod vw;
mod vw_preserve;

use std::sync::Arc;

use geo::Simplify;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::error::GeoArrowResult;
use shared::simplify_with;
pub use vw::simplify_vw;
pub use vw_preserve::simplify_vw_preserve;

pub fn simplify(array: &dyn GeoArrowArray, epsilon: f64) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    simplify_with(
        array,
        epsilon,
        &|ring: &geo::LineString<f64>, eps: f64| ring.simplify_idx(eps),
        &|poly: &geo::Polygon<f64>, eps: f64| Simplify::simplify_idx(poly, eps),
    )
}

#[cfg(test)]
mod test {
    use geo_traits::MultiLineStringTrait;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::cast::AsGeoArrowArray;
    use geoarrow_schema::CoordType;

    use super::*;
    use crate::dim_geom::all_coords;
    use crate::test_util::{
        NEAR_COLLINEAR_EPS, NEAR_COLLINEAR_KEPT, NEAR_COLLINEAR_LS, PLAIN_SQUARE, polygon_coords,
        xyzm_linestring_array, xyzm_multilinestring_array, xyzm_polygon_array,
    };

    #[test]
    fn linestring_maps_zm_of_kept_coords() {
        let arr = xyzm_linestring_array(&[&NEAR_COLLINEAR_LS]);
        let result = simplify(&arr, NEAR_COLLINEAR_EPS).unwrap();
        let out = result.as_line_string();
        assert_eq!(all_coords(&out.value(0).unwrap()), NEAR_COLLINEAR_KEPT);
    }

    /// `geo` never simplifies a ring below four coordinates. Every coordinate stays,
    /// even at a large epsilon.
    #[test]
    fn polygon_keeps_geo_ring_minimum() {
        let arr = xyzm_polygon_array(&[(&PLAIN_SQUARE, &[])]);
        let result = simplify(&arr, 100.0).unwrap();
        let out = result.as_polygon();
        assert_eq!(polygon_coords(&out.value(0).unwrap()), [PLAIN_SQUARE]);
    }

    #[test]
    fn multilinestring_maps_zm_of_kept_coords() {
        let arr = xyzm_multilinestring_array(&[&[&NEAR_COLLINEAR_LS]]);
        let result = simplify(&arr, NEAR_COLLINEAR_EPS).unwrap();
        let out = result.as_multi_line_string();
        let member = out.value(0).unwrap().line_string(0).unwrap();
        assert_eq!(all_coords(&member), NEAR_COLLINEAR_KEPT);
    }

    #[test]
    fn empty_polygon_stays_empty() {
        let arr = xyzm_polygon_array(&[(&[], &[])]);
        let result = simplify(&arr, 1.0).unwrap();
        let out = result.as_polygon();
        assert!(polygon_coords(&out.value(0).unwrap()).is_empty());
    }

    /// At an epsilon of zero no coordinate is removed, thus the two sets must match.
    #[test]
    fn geometry_array_preserves_zm() {
        let arr = geoarrow_array::test::geometry::array(CoordType::Separated, false);
        let result = simplify(&arr, 0.0).unwrap();
        let out = result.as_geometry();
        assert_eq!(out.len(), arr.len());

        for (input, output) in arr.iter().zip(out.iter()) {
            match (input, output) {
                (Some(Ok(in_g)), Some(Ok(out_g))) => {
                    assert_eq!(all_coords(&in_g), all_coords(&out_g));
                }
                (None, None) => {}
                _ => panic!("the output null positions do not match the input"),
            }
        }
    }
}
