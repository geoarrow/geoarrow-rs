use arrow_array::Float64Array;
use geo::line_measures::FrechetDistance;
use geo::{Distance, Euclidean, HasDimensions, HausdorffDistance};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowResult;

use crate::util::downcast::downcast_geoarrow_array_two_args;
use crate::util::{check_same_len, map_pair_to_f64};

/// Element-wise Euclidean distance between paired geometries.
pub fn euclidean_distance(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    check_same_len(left_array, right_array)?;
    downcast_geoarrow_array_two_args!(left_array, right_array, distance_impl)
}

/// Element-wise Hausdorff distance: the greatest of all distances from a point in
/// one geometry to the nearest point in the other.
///
/// A row with an empty geometry on either side is null, because `geo` has no
/// coordinates to measure and returns a large sentinel value instead.
pub fn hausdorff_distance(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    check_same_len(left_array, right_array)?;
    downcast_geoarrow_array_two_args!(left_array, right_array, hausdorff_impl)
}

/// Element-wise discrete Frechet distance.
///
/// `geo` defines this distance between two `LineString`s only. Any other pairing
/// gives a null row, which a caller cannot tell apart from a null input.
pub fn frechet_distance(
    left_array: &dyn GeoArrowArray,
    right_array: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    check_same_len(left_array, right_array)?;
    downcast_geoarrow_array_two_args!(left_array, right_array, frechet_impl)
}

fn distance_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    map_pair_to_f64(left_array, right_array, |left, right| {
        Some(Euclidean.distance(left, right))
    })
}

fn hausdorff_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    map_pair_to_f64(left_array, right_array, |left, right| {
        (!left.is_empty() && !right.is_empty()).then(|| left.hausdorff_distance(right))
    })
}

fn frechet_impl<'a>(
    left_array: &'a impl GeoArrowArrayAccessor<'a>,
    right_array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    map_pair_to_f64(left_array, right_array, |left, right| match (left, right) {
        (geo::Geometry::LineString(left), geo::Geometry::LineString(right)) => {
            Some(Euclidean.frechet_distance(left, right))
        }
        _ => None,
    })
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use geo::{Geometry, LineString, point};

    use super::*;
    use crate::test_util::geometry_array;

    /// An empty geometry nulls its row rather than taking `geo`'s sentinel value.
    #[test]
    fn hausdorff_points_and_empty() {
        let left = geometry_array(vec![
            Some(Geometry::from(point!(x: 0., y: 0.))),
            None,
            Some(Geometry::LineString(LineString::new(vec![]))),
        ]);
        let right = geometry_array(vec![
            Some(Geometry::from(point!(x: 3., y: 4.))),
            Some(Geometry::from(point!(x: 1., y: 1.))),
            Some(Geometry::from(point!(x: 0., y: 0.))),
        ]);

        let result = hausdorff_distance(&left, &right).unwrap();
        assert_eq!(result.value(0), 5.0);
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn frechet_nulls_non_linestring_pairs() {
        let a = LineString::from(vec![(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]);
        let b = LineString::from(vec![(0.0, 1.0), (1.0, 1.0), (2.0, 1.0)]);
        let left = geometry_array(vec![
            Some(Geometry::LineString(a.clone())),
            Some(Geometry::from(point!(x: 0., y: 0.))),
        ]);
        let right = geometry_array(vec![
            Some(Geometry::LineString(b.clone())),
            Some(Geometry::from(point!(x: 1., y: 1.))),
        ]);

        let result = frechet_distance(&left, &right).unwrap();
        assert_eq!(result.value(0), Euclidean.frechet_distance(&a, &b));
        assert!(result.is_null(1));
    }

    #[test]
    fn length_mismatch_errors() {
        let left = geometry_array(vec![Some(Geometry::from(point!(x: 0., y: 0.)))]);
        let right = geometry_array(vec![
            Some(Geometry::from(point!(x: 0., y: 0.))),
            Some(Geometry::from(point!(x: 1., y: 1.))),
        ]);
        assert!(hausdorff_distance(&left, &right).is_err());
        assert!(frechet_distance(&left, &right).is_err());
    }
}
