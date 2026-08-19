use arrow_array::Float64Array;
use geo::LineLocatePoint;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowResult;

use crate::util::downcast::downcast_geoarrow_array_two_args;
use crate::util::{check_same_len, map_pair_to_f64};

/// Element-wise: locate each point along its paired line, as the fraction of the
/// line's length at the nearest point on that line.
///
/// `geo` implements this for a `Line` or `LineString` with a `Point` argument only.
/// Any other pairing gives a null row, which a caller cannot tell apart from a
/// null input.
pub fn line_locate_point(
    lines: &dyn GeoArrowArray,
    points: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    check_same_len(lines, points)?;
    downcast_geoarrow_array_two_args!(lines, points, line_locate_point_impl)
}

fn line_locate_point_impl<'a>(
    lines: &'a impl GeoArrowArrayAccessor<'a>,
    points: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    map_pair_to_f64(lines, points, |line, point| match (line, point) {
        (geo::Geometry::LineString(line), geo::Geometry::Point(point)) => {
            line.line_locate_point(point)
        }
        (geo::Geometry::Line(line), geo::Geometry::Point(point)) => line.line_locate_point(point),
        _ => None,
    })
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use geo::{Geometry, LineString, point};

    use super::*;
    use crate::test_util::geometry_array;

    fn line(coords: Vec<(f64, f64)>) -> Option<Geometry> {
        Some(Geometry::LineString(LineString::from(coords)))
    }

    #[test]
    fn locate_midpoint_and_nulls() {
        let lines = geometry_array(vec![
            line(vec![(0.0, 0.0), (10.0, 0.0)]),
            Some(Geometry::from(point!(x: 0., y: 0.))),
            None,
            line(vec![(1.0, 1.0), (1.0, 1.0)]),
        ]);
        let points = geometry_array(vec![
            Some(Geometry::from(point!(x: 5., y: 5.))),
            Some(Geometry::from(point!(x: 1., y: 1.))),
            Some(Geometry::from(point!(x: 1., y: 1.))),
            Some(Geometry::from(point!(x: 5., y: 5.))),
        ]);

        let result = line_locate_point(&lines, &points).unwrap();
        assert_eq!(result.value(0), 0.5);
        assert!(result.is_null(1));
        assert!(result.is_null(2));
        // geo measures a zero-length line as Some(0.0), thus not a null row.
        assert_eq!(result.value(3), 0.0);
    }

    #[test]
    fn length_mismatch_errors() {
        let lines = geometry_array(vec![line(vec![(0.0, 0.0), (1.0, 0.0)])]);
        let points = geometry_array(vec![
            Some(Geometry::from(point!(x: 0., y: 0.))),
            Some(Geometry::from(point!(x: 1., y: 1.))),
        ]);
        assert!(line_locate_point(&lines, &points).is_err());
    }
}
