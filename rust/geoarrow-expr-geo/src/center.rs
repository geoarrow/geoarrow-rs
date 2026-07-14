use geo::BoundingRect;
use geoarrow_array::array::PointArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::CoordType;
use geoarrow_schema::error::GeoArrowResult;

use crate::util::map_to_point;

/// The midpoint of the envelope of each geometry, which is not the centroid.
///
/// A geometry with no coordinates has no envelope, thus its row is null.
pub fn center(array: &dyn GeoArrowArray, coord_type: CoordType) -> GeoArrowResult<PointArray> {
    downcast_geoarrow_array!(array, center_impl, coord_type)
}

fn center_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<PointArray> {
    map_to_point(array, coord_type, |geom| {
        geom.bounding_rect()
            .map(|rect| geo::Point::from(rect.center()))
    })
}

#[cfg(test)]
mod test {
    use geo::{Geometry, LineString, MultiPoint, Point, polygon};
    use geoarrow_schema::Dimension;

    use super::*;
    use crate::test_util::{PLAIN_SQUARE, geometry_array, point_at, xyzm_polygon_array};

    /// A right triangle with an envelope of `(0,0)-(4,2)` has its center at
    /// `(2,1)` but its centroid at `(4/3, 2/3)`.
    #[test]
    fn the_center_is_not_the_centroid() {
        let tri = Geometry::from(polygon![(x: 0., y: 0.), (x: 4., y: 0.), (x: 0., y: 2.)]);
        let arr = geometry_array(vec![Some(tri)]);

        let result = center(&arr, CoordType::Interleaved).unwrap();
        assert_eq!(point_at(&result, 0), Point::new(2.0, 1.0));
    }

    #[test]
    fn null_and_empty_produce_null() {
        let arr = geometry_array(vec![
            None,
            Some(Geometry::MultiPoint(MultiPoint::new(vec![]))),
            Some(Geometry::LineString(LineString::from(vec![
                (0.0, 0.0),
                (10.0, 4.0),
            ]))),
        ]);

        let result = center(&arr, CoordType::Interleaved).unwrap();
        assert!(result.iter().next().unwrap().is_none());
        assert!(result.iter().nth(1).unwrap().is_none());
        assert_eq!(point_at(&result, 2), Point::new(5.0, 2.0));
    }

    /// The midpoint is a new coordinate with no Z or M of its own, thus an XYZM
    /// input gives an XY output measured on x and y.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let arr = xyzm_polygon_array(&[(&PLAIN_SQUARE, &[])]);

        let result = center(&arr, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
        assert_eq!(point_at(&result, 0), Point::new(105.0, 105.0));
    }
}
