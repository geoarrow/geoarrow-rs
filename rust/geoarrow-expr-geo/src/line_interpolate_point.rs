use geo::{Euclidean, InterpolatableLine};
use geoarrow_array::array::PointArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::CoordType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::map_to_point;

/// Element-wise: the point `fraction` of the way along each line, by arc length.
/// `geo` holds a fraction outside `[0, 1]` at the nearer end.
///
/// `fraction` must be finite. `geo` interpolates a `Line` or a `LineString`
/// only. Any other geometry or an empty line gives a null row, which a caller
/// cannot tell apart from a null input.
pub fn line_interpolate_point(
    array: &dyn GeoArrowArray,
    fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    if !fraction.is_finite() {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "line_interpolate_point fraction must be a finite value, got {fraction}"
        )));
    }
    downcast_geoarrow_array!(array, line_interpolate_point_impl, fraction, coord_type)
}

fn line_interpolate_point_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    map_to_point(array, coord_type, |geom| {
        let point = match geom {
            geo::Geometry::LineString(ls) => ls.point_at_ratio_from_start(&Euclidean, fraction),
            geo::Geometry::Line(l) => Some(l.point_at_ratio_from_start(&Euclidean, fraction)),
            _ => None,
        };
        point.filter(|p| p.x().is_finite() && p.y().is_finite())
    })
}

#[cfg(test)]
mod test {
    use geo::{Geometry, LineString, Point, point};
    use geoarrow_array::builder::LineStringBuilder;
    use geoarrow_schema::{Dimension, LineStringType};

    use super::*;
    use crate::test_util::{geometry_array, point_at, xyzm_linestring_array};

    fn line_string_array(line_string: LineString) -> geoarrow_array::array::LineStringArray {
        let typ = LineStringType::new(Dimension::XY, Default::default())
            .with_coord_type(CoordType::Interleaved);
        let mut builder = LineStringBuilder::new(typ);
        builder.push_line_string(Some(&line_string)).unwrap();
        builder.finish()
    }

    #[test]
    fn a_fraction_of_zero_a_half_and_one() {
        let arr = line_string_array(LineString::from(vec![(0.0, 0.0), (10.0, 0.0)]));
        let interpolate = |fraction| {
            let result = line_interpolate_point(&arr, fraction, CoordType::Interleaved).unwrap();
            point_at(&result, 0)
        };

        assert_eq!(interpolate(0.0), Point::new(0.0, 0.0));
        assert_eq!(interpolate(0.5), Point::new(5.0, 0.0));
        assert_eq!(interpolate(1.0), Point::new(10.0, 0.0));
    }

    #[test]
    fn a_null_and_a_geometry_that_is_not_a_line_produce_null() {
        let arr = geometry_array(vec![
            None,
            Some(Geometry::from(point!(x: 1., y: 2.))),
            Some(Geometry::LineString(LineString::from(vec![
                (0.0, 0.0),
                (4.0, 0.0),
            ]))),
        ]);

        let result = line_interpolate_point(&arr, 0.5, CoordType::Interleaved).unwrap();
        assert!(result.iter().next().unwrap().is_none());
        assert!(result.iter().nth(1).unwrap().is_none());
        assert_eq!(point_at(&result, 2), Point::new(2.0, 0.0));
    }

    /// An empty line has nothing to interpolate and gives a null row. A fraction
    /// that is not finite is rejected up front, as in densify and concave_hull.
    #[test]
    fn an_empty_line_produces_null_and_a_non_finite_fraction_errors() {
        let empty = line_string_array(LineString::new(vec![]));
        let result = line_interpolate_point(&empty, 0.5, CoordType::Interleaved).unwrap();
        assert!(result.iter().next().unwrap().is_none());

        let arr = line_string_array(LineString::from(vec![(0.0, 0.0), (10.0, 0.0)]));
        assert!(line_interpolate_point(&arr, f64::NAN, CoordType::Interleaved).is_err());
    }

    /// The interpolated point is a new coordinate with no Z or M of its own, thus
    /// an XYZM input gives an XY output measured on x and y.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let arr = xyzm_linestring_array(&[&[[0.0, 0.0, 10.0, 100.0], [10.0, 0.0, 11.0, 101.0]]]);

        let result = line_interpolate_point(&arr, 0.5, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
        assert_eq!(point_at(&result, 0), Point::new(5.0, 0.0));
    }
}
