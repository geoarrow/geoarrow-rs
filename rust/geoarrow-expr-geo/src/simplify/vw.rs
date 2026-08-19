use std::sync::Arc;

use geo::SimplifyVw;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::error::GeoArrowResult;

use super::shared::simplify_with;
use crate::util::copy_geoarrow_array_ref;

pub fn simplify_vw(
    array: &dyn GeoArrowArray,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    // `geo`'s coordinate path guards on this, but the index path does not, because it
    // removes triangles with zero area. A NaN epsilon continues to the algorithm.
    if epsilon <= 0.0 {
        return Ok(copy_geoarrow_array_ref(array));
    }
    simplify_with(
        array,
        epsilon,
        &|ring: &geo::LineString<f64>, eps: f64| ring.simplify_vw_idx(eps),
        &|poly: &geo::Polygon<f64>, eps: f64| poly.simplify_vw_idx(eps),
    )
}

#[cfg(test)]
mod test {
    use geo_traits::LineStringTrait;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::cast::AsGeoArrowArray;

    use super::*;
    use crate::dim_geom::all_coords;
    use crate::test_util::xyzm_linestring_array;

    #[test]
    fn zero_epsilon_keeps_collinear_coords() {
        let coords: [[f64; 4]; 3] = [
            [0.0, 0.0, 10.0, 100.0],
            [1.0, 1.0, 11.0, 101.0],
            [2.0, 2.0, 12.0, 102.0],
        ];
        let arr = xyzm_linestring_array(&[&coords]);
        let result = simplify_vw(&arr, 0.0).unwrap();
        let out = result.as_line_string();
        assert_eq!(all_coords(&out.value(0).unwrap()), coords);
    }

    /// Both shoulders are within the RDP epsilon, but they bound a triangle large enough
    /// for VW. RDP removes them and VW keeps them.
    #[test]
    fn uses_vw_not_rdp() {
        let ls: [[f64; 4]; 5] = [
            [0.0, 0.0, 10.0, 100.0],
            [1.0, 0.6, 11.0, 101.0],
            [5.0, 1.0, 12.0, 102.0],
            [9.0, 0.6, 13.0, 103.0],
            [10.0, 0.0, 14.0, 104.0],
        ];
        let arr = xyzm_linestring_array(&[&ls]);

        let rdp = crate::simplify::simplify(&arr, 0.8).unwrap();
        assert_eq!(
            rdp.as_line_string().value(0).unwrap().num_coords(),
            3,
            "RDP must remove both shoulders, or this test proves nothing"
        );

        let result = simplify_vw(&arr, 0.8).unwrap();
        let out = result.as_line_string();
        assert_eq!(all_coords(&out.value(0).unwrap()), ls);
    }
}
