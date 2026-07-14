use std::sync::Arc;

use geo::ChaikinSmoothing;
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::CoordType;
use geoarrow_schema::error::GeoArrowResult;

use crate::util::map_geometry;

/// Cut the corners of each geometry `n_iterations` times, by Chaikin's
/// algorithm. Each iteration about doubles the number of vertices.
///
/// An open line string keeps its first and last vertex. A closed ring has its
/// start corner cut with all the others.
pub fn chaikin_smoothing(
    array: &dyn GeoArrowArray,
    n_iterations: usize,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    map_geometry(array, coord_type, &|geom| {
        chaikin_geometry(geom, n_iterations)
    })
}

/// `geo` clones a collection instead of smoothing its members, thus this
/// function recurses into one. Every other geometry `geo` handles itself.
fn chaikin_geometry(geom: &geo::Geometry, n_iterations: usize) -> geo::Geometry {
    match geom {
        geo::Geometry::GeometryCollection(gc) => {
            geo::Geometry::GeometryCollection(geo::GeometryCollection::new_from(
                gc.iter()
                    .map(|member| chaikin_geometry(member, n_iterations))
                    .collect(),
            ))
        }
        other => other.chaikin_smoothing(n_iterations),
    }
}

#[cfg(test)]
mod test {
    use geo::{Geometry, LineString, point};
    use geoarrow_array::cast::AsGeoArrowArray;
    use geoarrow_schema::Dimension;

    use super::*;
    use crate::test_util::{
        PLAIN_SQUARE, geometry_array, geometry_collection_array, read_geoms, xyzm_polygon_array,
    };

    fn corner_line() -> LineString {
        LineString::from(vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)])
    }

    /// An open line string keeps its end points and gains a pair of vertices at
    /// each quarter point of a segment.
    #[test]
    fn an_open_line_string_keeps_its_end_points() {
        let arr = geometry_array(vec![Some(Geometry::LineString(corner_line()))]);

        let result = chaikin_smoothing(&arr, 1, CoordType::Interleaved).unwrap();
        let expected = LineString::from(vec![
            (0.0, 0.0),
            (2.5, 0.0),
            (7.5, 0.0),
            (10.0, 2.5),
            (10.0, 7.5),
            (10.0, 10.0),
        ]);
        assert_eq!(
            read_geoms(result.as_geometry()),
            [Some(Geometry::LineString(expected))]
        );
    }

    /// A point has no corner to cut, and zero iterations change nothing.
    #[test]
    fn null_point_and_zero_iterations_pass_through() {
        let geoms = vec![
            None,
            Some(Geometry::from(point!(x: 5., y: 7.))),
            Some(Geometry::LineString(corner_line())),
        ];
        let arr = geometry_array(geoms.clone());

        let result = chaikin_smoothing(&arr, 0, CoordType::Interleaved).unwrap();
        assert_eq!(read_geoms(result.as_geometry()), geoms);
    }

    /// A cut corner is a new vertex with no Z or M of its own, thus an XYZM input
    /// gives an XY output. The polygon array keeps its type.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let arr = xyzm_polygon_array(&[(&PLAIN_SQUARE, &[])]);

        let result = chaikin_smoothing(&arr, 1, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
        let Some(Geometry::Polygon(out)) = &read_geoms(result.as_polygon())[0] else {
            panic!("a polygon array must stay a polygon array");
        };
        assert_eq!(out.exterior().0.len(), 9);
    }

    /// `geo` clones a collection whole, thus the recursion of `chaikin_geometry`
    /// is what smooths a member.
    #[test]
    fn a_geometry_collection_smooths_its_members() {
        let gc = geo::GeometryCollection::new_from(vec![
            Geometry::LineString(corner_line()),
            Geometry::Point(point!(x: 100., y: 100.)),
        ]);

        let result =
            chaikin_smoothing(&geometry_collection_array(&gc), 1, CoordType::Interleaved).unwrap();
        let expected = geo::GeometryCollection::new_from(vec![
            Geometry::LineString(corner_line().chaikin_smoothing(1)),
            Geometry::Point(point!(x: 100., y: 100.)),
        ]);
        assert_eq!(
            read_geoms(result.as_geometry()),
            [Some(Geometry::GeometryCollection(expected))]
        );
    }
}
