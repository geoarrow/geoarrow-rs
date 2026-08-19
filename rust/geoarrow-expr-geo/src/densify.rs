use std::sync::Arc;

use geo::{Densify, Euclidean};
use geoarrow_array::GeoArrowArray;
use geoarrow_schema::CoordType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::util::map_geometry;

/// Add interpolated vertices to each geometry until no two adjacent vertices are
/// more than `max_distance` apart, in the Euclidean metric.
///
/// `max_distance` must be finite and greater than zero. `geo` panics on a
/// non-positive distance, and only for a geometry that has a segment, thus this
/// kernel rejects the bad value up front for one uniform error.
pub fn densify(
    array: &dyn GeoArrowArray,
    max_distance: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    if !max_distance.is_finite() || max_distance <= 0.0 {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "densify max_distance must be a finite value greater than 0, got {max_distance}"
        )));
    }
    map_geometry(array, coord_type, &|geom| {
        densify_geometry(geom, max_distance)
    })
}

/// A densified rect or triangle no longer fits its own type, thus both become a
/// polygon. `geo` has no `Densify` for `Geometry`.
fn densify_geometry(geom: &geo::Geometry, max_distance: f64) -> geo::Geometry {
    use geo::Geometry;
    match geom {
        Geometry::Point(g) => Geometry::Point(*g),
        Geometry::MultiPoint(g) => Geometry::MultiPoint(g.clone()),
        Geometry::Line(g) => Geometry::LineString(Euclidean.densify(g, max_distance)),
        Geometry::LineString(g) => Geometry::LineString(Euclidean.densify(g, max_distance)),
        Geometry::MultiLineString(g) => {
            Geometry::MultiLineString(Euclidean.densify(g, max_distance))
        }
        Geometry::Polygon(g) => Geometry::Polygon(Euclidean.densify(g, max_distance)),
        Geometry::MultiPolygon(g) => Geometry::MultiPolygon(Euclidean.densify(g, max_distance)),
        Geometry::Rect(g) => Geometry::Polygon(Euclidean.densify(g, max_distance)),
        Geometry::Triangle(g) => Geometry::Polygon(Euclidean.densify(g, max_distance)),
        Geometry::GeometryCollection(g) => {
            Geometry::GeometryCollection(geo::GeometryCollection::new_from(
                g.iter()
                    .map(|member| densify_geometry(member, max_distance))
                    .collect(),
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use geo::{Geometry, LineString, point};
    use geoarrow_array::builder::LineStringBuilder;
    use geoarrow_array::cast::AsGeoArrowArray;
    use geoarrow_schema::{Dimension, LineStringType};

    use super::*;
    use crate::test_util::{
        PLAIN_SQUARE, geometry_array, geometry_collection_array, read_geoms, xyzm_polygon_array,
    };

    fn line(coords: Vec<(f64, f64)>) -> Geometry {
        Geometry::LineString(LineString::from(coords))
    }

    /// A line string array keeps its type through the kernel, and each 6 unit
    /// edge gains two vertices at a maximum distance of 2.
    #[test]
    fn a_line_string_array_stays_a_line_string_array() {
        let typ = LineStringType::new(Dimension::XY, Default::default())
            .with_coord_type(CoordType::Interleaved);
        let mut builder = LineStringBuilder::new(typ);
        builder
            .push_line_string(Some(&LineString::from(vec![(0.0, 0.0), (0.0, 6.0)])))
            .unwrap();

        let result = densify(&builder.finish(), 2.0, CoordType::Interleaved).unwrap();
        let expected = line(vec![(0.0, 0.0), (0.0, 2.0), (0.0, 4.0), (0.0, 6.0)]);
        assert_eq!(read_geoms(result.as_line_string()), [Some(expected)]);
    }

    /// A point has no segment, thus it passes through. An empty line string has
    /// none either, thus it stays empty instead of tripping the `geo` assertion.
    #[test]
    fn null_point_and_empty_pass_through() {
        let arr = geometry_array(vec![
            None,
            Some(Geometry::from(point!(x: 5., y: 7.))),
            Some(Geometry::LineString(LineString::new(vec![]))),
            Some(line(vec![(0.0, 0.0), (0.0, 4.0)])),
        ]);

        let result = densify(&arr, 2.0, CoordType::Interleaved).unwrap();
        assert_eq!(
            read_geoms(result.as_geometry()),
            [
                None,
                Some(Geometry::from(point!(x: 5., y: 7.))),
                Some(Geometry::LineString(LineString::new(vec![]))),
                Some(line(vec![(0.0, 0.0), (0.0, 2.0), (0.0, 4.0)])),
            ]
        );
    }

    /// A new vertex has no Z or M of its own, thus an XYZM input gives an XY
    /// output. Each 10 unit edge of the square gains four vertices.
    #[test]
    fn xyzm_input_gives_xy_output() {
        let arr = xyzm_polygon_array(&[(&PLAIN_SQUARE, &[])]);

        let result = densify(&arr, 2.0, CoordType::Separated).unwrap();
        assert_eq!(result.data_type().dimension(), Some(Dimension::XY));
        let Some(Geometry::Polygon(poly)) = &read_geoms(result.as_polygon())[0] else {
            panic!("a polygon array must stay a polygon array");
        };
        assert_eq!(poly.exterior().0.len(), 21);
    }

    /// `geo` has no `Densify` for a collection, thus each member is densified on
    /// its own.
    #[test]
    fn a_geometry_collection_densifies_its_members() {
        let gc = geo::GeometryCollection::new_from(vec![
            line(vec![(0.0, 0.0), (0.0, 6.0)]),
            Geometry::Point(point!(x: 100., y: 100.)),
        ]);

        let result = densify(&geometry_collection_array(&gc), 2.0, CoordType::Interleaved).unwrap();
        let expected = geo::GeometryCollection::new_from(vec![
            line(vec![(0.0, 0.0), (0.0, 2.0), (0.0, 4.0), (0.0, 6.0)]),
            Geometry::Point(point!(x: 100., y: 100.)),
        ]);
        assert_eq!(
            read_geoms(result.as_geometry()),
            [Some(Geometry::GeometryCollection(expected))]
        );
    }

    /// A rect array has no builder of its own, thus each rect becomes a polygon
    /// with two extra vertices per 4 unit edge.
    #[test]
    fn a_rect_array_lowers_to_a_polygon() {
        use geo::{Rect, coord};
        use geoarrow_array::builder::RectBuilder;
        use geoarrow_schema::BoxType;

        let mut builder = RectBuilder::new(BoxType::new(Dimension::XY, Default::default()));
        builder.push_rect(Some(&Rect::new(
            coord! { x: 0., y: 0. },
            coord! { x: 4., y: 4. },
        )));

        let result = densify(&builder.finish(), 2.0, CoordType::Interleaved).unwrap();
        let Some(Geometry::Polygon(poly)) = &read_geoms(result.as_geometry())[0] else {
            panic!("a rect must lower to a polygon");
        };
        assert_eq!(poly.exterior().0.len(), 9);
    }

    #[test]
    fn non_positive_distance_errors() {
        let arr = geometry_array(vec![Some(line(vec![(0.0, 0.0), (0.0, 4.0)]))]);
        assert!(densify(&arr, 0.0, CoordType::Interleaved).is_err());
        assert!(densify(&arr, -1.0, CoordType::Interleaved).is_err());
        assert!(densify(&arr, f64::NAN, CoordType::Interleaved).is_err());
    }
}
