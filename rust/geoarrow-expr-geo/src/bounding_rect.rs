use geo::BoundingRect;
use geoarrow_array::array::PolygonArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::CoordType;
use geoarrow_schema::error::GeoArrowResult;

use crate::util::map_to_polygon;

/// The envelope of each geometry, as a polygon.
///
/// A geometry with no coordinates has no envelope, thus its row is null.
pub fn bounding_rect(
    array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    downcast_geoarrow_array!(array, bounding_rect_impl, coord_type)
}

fn bounding_rect_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<PolygonArray> {
    map_to_polygon(array, coord_type, |geom| {
        geom.bounding_rect().map(|rect| rect.to_polygon())
    })
}

#[cfg(test)]
mod test {
    use geo::{Geometry, MultiPoint, point, polygon};
    use geoarrow_schema::Dimension;

    use super::*;
    use crate::test_util::{PLAIN_SQUARE, geometry_array, polygon_at, xyzm_polygon_array};

    /// `Rect::to_polygon` starts the ring at the `(max_x, min_y)` corner and runs
    /// counter clockwise.
    #[test]
    fn envelope_of_a_triangle() {
        let tri = Geometry::from(polygon![(x: 0., y: 0.), (x: 4., y: 0.), (x: 2., y: 3.)]);
        let arr = geometry_array(vec![Some(tri)]);

        let result = bounding_rect(&arr, CoordType::Interleaved).unwrap();
        let expected = polygon![(x: 4., y: 0.), (x: 4., y: 3.), (x: 0., y: 3.), (x: 0., y: 0.)];
        assert_eq!(polygon_at(&result, 0), expected);
    }

    #[test]
    fn null_and_empty_produce_null() {
        let arr = geometry_array(vec![
            None,
            Some(Geometry::MultiPoint(MultiPoint::new(vec![]))),
            Some(Geometry::from(point!(x: 5., y: 7.))),
        ]);

        let result = bounding_rect(&arr, CoordType::Interleaved).unwrap();
        let rows: Vec<_> = result.iter().collect();
        assert!(rows[0].is_none());
        assert!(rows[1].is_none());
        assert!(rows[2].is_some());
    }

    /// The four corners are new coordinates with no Z or M of their own, thus an
    /// XYZM input gives an XY output measured on x and y.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let arr = xyzm_polygon_array(&[(&PLAIN_SQUARE, &[])]);

        let result = bounding_rect(&arr, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
        let expected = polygon![
            (x: 110., y: 100.), (x: 110., y: 110.), (x: 100., y: 110.), (x: 100., y: 100.)
        ];
        assert_eq!(polygon_at(&result, 0), expected);
    }
}
