use geo::{Closest, ClosestPoint};
use geoarrow_array::array::PointArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::CoordType;
use geoarrow_schema::error::GeoArrowResult;

use crate::util::downcast::downcast_geoarrow_array_two_args;
use crate::util::{check_same_len, map_pair_to_point};

/// Element-wise: the point of each geometry closest to its paired query point.
///
/// `geo` takes a point as the query and reports two or more equally close points
/// as `Closest::Indeterminate`. Both a query that is not a point and an
/// indeterminate answer give a null row, which a caller cannot tell apart from a
/// null input.
pub fn closest_point(
    geoms: &dyn GeoArrowArray,
    points: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    check_same_len(geoms, points)?;
    downcast_geoarrow_array_two_args!(geoms, points, closest_point_impl, coord_type)
}

fn closest_point_impl<'a>(
    geoms: &'a impl GeoArrowArrayAccessor<'a>,
    points: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    map_pair_to_point(geoms, points, coord_type, |geom, query| match query {
        geo::Geometry::Point(query) => match geom.closest_point(query) {
            Closest::Intersection(p) | Closest::SinglePoint(p) => Some(p),
            Closest::Indeterminate => None,
        },
        _ => None,
    })
}

#[cfg(test)]
mod test {
    use geo::{Geometry, LineString, Point, point};
    use geoarrow_schema::Dimension;

    use super::*;
    use crate::test_util::{geometry_array, point_at, xyzm_linestring_array};

    fn line(coords: Vec<(f64, f64)>) -> Option<Geometry> {
        Some(Geometry::LineString(LineString::from(coords)))
    }

    /// A query off the line gives the foot of the perpendicular, a query on it
    /// gives the query itself, and a zero length line gives
    /// `Closest::Indeterminate`, thus a null row.
    #[test]
    fn closest_points_and_an_indeterminate_one() {
        let geoms = geometry_array(vec![
            line(vec![(0.0, 0.0), (10.0, 0.0)]),
            line(vec![(0.0, 0.0), (10.0, 0.0)]),
            line(vec![(0.0, 0.0), (0.0, 0.0)]),
        ]);
        let points = geometry_array(vec![
            Some(Geometry::from(point!(x: 5., y: 5.))),
            Some(Geometry::from(point!(x: 0., y: 0.))),
            Some(Geometry::from(point!(x: 5., y: 5.))),
        ]);

        let result = closest_point(&geoms, &points, CoordType::Interleaved).unwrap();
        assert_eq!(point_at(&result, 0), Point::new(5.0, 0.0));
        assert_eq!(point_at(&result, 1), Point::new(0.0, 0.0));
        assert!(result.iter().nth(2).unwrap().is_none());
    }

    #[test]
    fn a_non_point_query_and_a_null_produce_null() {
        let geoms = geometry_array(vec![line(vec![(0.0, 0.0), (10.0, 0.0)]), None]);
        let points = geometry_array(vec![
            line(vec![(0.0, 0.0), (1.0, 1.0)]),
            Some(Geometry::from(point!(x: 1., y: 1.))),
        ]);

        let result = closest_point(&geoms, &points, CoordType::Interleaved).unwrap();
        assert!(result.iter().next().unwrap().is_none());
        assert!(result.iter().nth(1).unwrap().is_none());
    }

    /// The closest point is a new coordinate with no Z or M of its own, thus an
    /// XYZM input gives an XY output measured on x and y.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let geoms = xyzm_linestring_array(&[&[[0.0, 0.0, 10.0, 100.0], [10.0, 0.0, 11.0, 101.0]]]);
        let points = geometry_array(vec![Some(Geometry::from(point!(x: 5., y: 5.)))]);

        let result = closest_point(&geoms, &points, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
        assert_eq!(point_at(&result, 0), Point::new(5.0, 0.0));
    }

    #[test]
    fn length_mismatch_errors() {
        let geoms = geometry_array(vec![Some(Geometry::from(point!(x: 0., y: 0.)))]);
        let points = geometry_array(vec![
            Some(Geometry::from(point!(x: 0., y: 0.))),
            Some(Geometry::from(point!(x: 1., y: 1.))),
        ]);
        assert!(closest_point(&geoms, &points, CoordType::Interleaved).is_err());
    }
}
