use geo::ConcaveHull;
use geo::algorithm::concave_hull::ConcaveHullOptions;
use geoarrow_array::array::PolygonArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::CoordType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::map_to_polygon;

/// The concave hull of each geometry. A smaller `concavity` traces a tighter
/// boundary, a larger one approaches the convex hull. A geometry variant `geo`
/// does not implement gives a null row, which a caller cannot tell apart from a
/// null input.
///
/// `concavity` must be finite and not negative. In a release build `geo` clamps
/// a negative value to zero and reads an infinite one as "give the convex hull",
/// thus this kernel rejects both for one deterministic error.
pub fn concave_hull(
    array: &dyn GeoArrowArray,
    concavity: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    if !concavity.is_finite() || concavity < 0.0 {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "concave_hull concavity must be a finite non-negative value, got {concavity}"
        )));
    }
    downcast_geoarrow_array!(array, concave_hull_impl, concavity, coord_type)
}

fn concave_hull_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    concavity: f64,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    map_to_polygon(array, coord_type, |geom| {
        concave_hull_geometry(geom, concavity)
    })
}

/// `geo` has no `ConcaveHull` for `Geometry`. A caller gets a null row for a
/// variant it does not implement, which it cannot tell apart from a null input.
fn concave_hull_geometry(geom: &geo::Geometry, concavity: f64) -> Option<geo::Polygon> {
    let options = ConcaveHullOptions::<f64>::default().concavity(concavity);
    use geo::Geometry;
    match geom {
        Geometry::LineString(g) => Some(g.concave_hull_with_options(options)),
        Geometry::Polygon(g) => Some(g.concave_hull_with_options(options)),
        Geometry::MultiPoint(g) => Some(g.concave_hull_with_options(options)),
        Geometry::MultiLineString(g) => Some(g.concave_hull_with_options(options)),
        Geometry::MultiPolygon(g) => Some(g.concave_hull_with_options(options)),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use geo::{Area, Geometry, MultiPoint, Point, point};
    use geoarrow_schema::Dimension;

    use super::*;
    use crate::test_util::{PLAIN_SQUARE, geometry_array, polygon_at, xyzm_polygon_array};

    /// A square with a notch at `(1.5, 1)`. `geo` drills into the notch at a
    /// small concavity and relaxes toward the convex hull at a large one.
    fn notched_multi_point() -> MultiPoint {
        MultiPoint::new(vec![
            Point::new(0.0, 0.0),
            Point::new(2.0, 0.0),
            Point::new(1.5, 1.0),
            Point::new(2.0, 2.0),
            Point::new(0.0, 2.0),
        ])
    }

    fn expected_hull(multi_point: &MultiPoint, concavity: f64) -> geo::Polygon {
        multi_point
            .concave_hull_with_options(ConcaveHullOptions::<f64>::default().concavity(concavity))
    }

    /// A hull that ignored `concavity` would come out the same at both values.
    #[test]
    fn concavity_reaches_geo() {
        let multi_point = notched_multi_point();
        let arr = geometry_array(vec![Some(Geometry::MultiPoint(multi_point.clone()))]);

        let tight = concave_hull(&arr, 1.0, CoordType::Interleaved).unwrap();
        let loose = concave_hull(&arr, 100.0, CoordType::Interleaved).unwrap();

        assert_eq!(polygon_at(&tight, 0), expected_hull(&multi_point, 1.0));
        assert!(
            polygon_at(&tight, 0).exterior().0.len() > polygon_at(&loose, 0).exterior().0.len()
        );
    }

    /// Row 3 is three collinear points, whose hull has zero area. It must reach
    /// the output as a present row, which pins that `PolygonBuilder` takes a
    /// degenerate ring.
    #[test]
    fn unsupported_variant_and_null_produce_null() {
        let collinear = MultiPoint::new(vec![
            Point::new(0.0, 0.0),
            Point::new(3.0, 3.0),
            Point::new(6.0, 6.0),
        ]);
        let arr = geometry_array(vec![
            Some(Geometry::from(point!(x: 5., y: 7.))),
            Some(Geometry::MultiPoint(notched_multi_point())),
            None,
            Some(Geometry::MultiPoint(collinear.clone())),
        ]);

        let result = concave_hull(&arr, 2.0, CoordType::Interleaved).unwrap();
        let rows: Vec<_> = result.iter().collect();
        assert!(rows[0].is_none());
        assert!(rows[1].is_some());
        assert!(rows[2].is_none());
        assert_eq!(polygon_at(&result, 3), expected_hull(&collinear, 2.0));
        assert!(polygon_at(&result, 3).unsigned_area() < 1e-9);
    }

    /// A hull is a two dimensional construct, thus an XYZM input gives an XY
    /// output.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let arr = xyzm_polygon_array(&[(&PLAIN_SQUARE, &[])]);

        let result = concave_hull(&arr, 2.0, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
    }

    #[test]
    fn negative_or_non_finite_concavity_errors() {
        let arr = geometry_array(vec![Some(Geometry::MultiPoint(notched_multi_point()))]);
        assert!(concave_hull(&arr, -1.0, CoordType::Interleaved).is_err());
        assert!(concave_hull(&arr, f64::NAN, CoordType::Interleaved).is_err());
    }
}
