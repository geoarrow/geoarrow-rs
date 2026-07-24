use std::sync::Arc;

use geo::SimplifyVwPreserve;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::error::GeoArrowResult;

use super::shared::simplify_with;

pub fn simplify_vw_preserve(
    array: &dyn GeoArrowArray,
    epsilon: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    // No epsilon guard, unlike `simplify_vw`: the vw-preserve index path in `geo`
    // returns the input unchanged for an epsilon of zero or less.
    simplify_with(
        array,
        epsilon,
        &|ring: &geo::LineString<f64>, eps: f64| ring.simplify_vw_preserve_idx(eps),
        &|poly: &geo::Polygon<f64>, eps: f64| poly.simplify_vw_preserve_idx(eps),
    )
}

#[cfg(test)]
mod test {
    use geo_traits::MultiPolygonTrait;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::cast::AsGeoArrowArray;
    use geoarrow_schema::Dimension;

    use super::*;
    use crate::test_util::{
        NOTCH_EPS, NOTCH_SQUARE, NOTCH_SQUARE_KEPT, PLAIN_SQUARE, polygon_coords,
        xyzm_multipolygon_array, xyzm_polygon_array,
    };

    const HOLE_A: [[f64; 4]; 5] = [
        [2.0, 2.0, 60.0, 600.0],
        [4.0, 2.0, 61.0, 601.0],
        [4.0, 4.0, 62.0, 602.0],
        [2.0, 4.0, 63.0, 603.0],
        [2.0, 2.0, 60.0, 600.0],
    ];
    const HOLE_B: [[f64; 4]; 5] = [
        [6.0, 6.0, 70.0, 700.0],
        [8.0, 6.0, 71.0, 701.0],
        [8.0, 8.0, 72.0, 702.0],
        [6.0, 8.0, 73.0, 703.0],
        [6.0, 6.0, 70.0, 700.0],
    ];

    /// The closing coordinate must come from this row, not from the next one.
    #[test]
    fn open_ring_closes_on_its_own_first_coord() {
        let open: [[f64; 4]; 4] = [
            [0.0, 0.0, 10.0, 100.0],
            [10.0, 0.0, 11.0, 101.0],
            [10.0, 10.0, 12.0, 102.0],
            [0.0, 10.0, 13.0, 103.0],
        ];
        let far: [[f64; 4]; 5] = [
            [999.0, 999.0, 50.0, 500.0],
            [1009.0, 999.0, 51.0, 501.0],
            [1009.0, 1009.0, 52.0, 502.0],
            [999.0, 1009.0, 53.0, 503.0],
            [999.0, 999.0, 50.0, 500.0],
        ];
        let arr = xyzm_polygon_array(&[(&open, &[]), (&far, &[])]);
        let result = simplify_vw_preserve(&arr, 1.0).unwrap();
        let out = result.as_polygon();

        let closed: [[f64; 4]; 5] = [open[0], open[1], open[2], open[3], open[0]];
        assert_eq!(polygon_coords(&out.value(0).unwrap()), [closed]);
    }

    /// Two holes, so a transposition of the interior ring indices would fail the test.
    #[test]
    fn polygon_with_holes_maps_zm_of_kept_coords() {
        let arr = xyzm_polygon_array(&[(&NOTCH_SQUARE, &[&HOLE_A, &HOLE_B])]);
        let result = simplify_vw_preserve(&arr, NOTCH_EPS).unwrap();
        let out = result.as_polygon();
        assert_eq!(
            polygon_coords(&out.value(0).unwrap()),
            [&NOTCH_SQUARE_KEPT[..], &HOLE_A[..], &HOLE_B[..]]
        );
    }

    #[test]
    fn multipolygon_maps_zm_of_kept_coords() {
        let arr = xyzm_multipolygon_array(&[&[(&NOTCH_SQUARE, &[]), (&PLAIN_SQUARE, &[])]]);
        let result = simplify_vw_preserve(&arr, NOTCH_EPS).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XYZM));
        let out = result.as_multi_polygon();
        let row0 = out.value(0).unwrap();

        assert_eq!(row0.num_polygons(), 2);
        assert_eq!(
            polygon_coords(&row0.polygon(0).unwrap()),
            [NOTCH_SQUARE_KEPT]
        );
        assert_eq!(polygon_coords(&row0.polygon(1).unwrap()), [PLAIN_SQUARE]);
    }
}
