use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use geo::algorithm::convex_hull::ConvexHull as GeoConvexHull;
use geo::Polygon;

/// Returns the convex hull of a Polygon. The hull is always oriented counter-clockwise.
///
/// This implementation uses the QuickHull algorithm,
/// based on [Barber, C. Bradford; Dobkin, David P.; Huhdanpaa, Hannu (1 December 1996)](https://dx.doi.org/10.1145%2F235815.235821)
/// Original paper here: <http://www.cs.princeton.edu/~dpd/Papers/BarberDobkinHuhdanpaa.pdf>
///
/// # Examples
///
/// ```
/// use geo::{line_string, polygon};
/// use geo::ConvexHull;
///
/// // an L shape
/// let poly = polygon![
///     (x: 0.0, y: 0.0),
///     (x: 4.0, y: 0.0),
///     (x: 4.0, y: 1.0),
///     (x: 1.0, y: 1.0),
///     (x: 1.0, y: 4.0),
///     (x: 0.0, y: 4.0),
///     (x: 0.0, y: 0.0),
/// ];
///
/// // The correct convex hull coordinates
/// let correct_hull = line_string![
///     (x: 4.0, y: 0.0),
///     (x: 4.0, y: 1.0),
///     (x: 1.0, y: 4.0),
///     (x: 0.0, y: 4.0),
///     (x: 0.0, y: 0.0),
///     (x: 4.0, y: 0.0),
/// ];
///
/// let res = poly.convex_hull();
/// assert_eq!(res.exterior(), &correct_hull);
/// ```
pub trait ConvexHull {
    fn convex_hull(&self) -> PolygonArray;
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ident) => {
        impl ConvexHull for $type {
            fn convex_hull(&self) -> PolygonArray {
                let output_geoms: Vec<Option<Polygon>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(PointArray);
iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(WKBArray);

impl ConvexHull for GeometryArray {
    fn convex_hull(&self) -> PolygonArray {
        match self {
            GeometryArray::WKB(arr) => arr.convex_hull(),
            GeometryArray::Point(arr) => arr.convex_hull(),
            GeometryArray::LineString(arr) => arr.convex_hull(),
            GeometryArray::Polygon(arr) => arr.convex_hull(),
            GeometryArray::MultiPoint(arr) => arr.convex_hull(),
            GeometryArray::MultiLineString(arr) => arr.convex_hull(),
            GeometryArray::MultiPolygon(arr) => arr.convex_hull(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConvexHull;
    use crate::array::{LineStringArray, MultiPointArray};
    use crate::GeometryArrayTrait;
    use geo::{line_string, polygon, MultiPoint, Point};

    #[test]
    fn convex_hull_for_multipoint() {
        // Values borrowed from this test in geo crate: https://docs.rs/geo/0.14.2/src/geo/algorithm/convexhull.rs.html#323
        let input_geom: MultiPoint = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 0.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ]
        .into();
        let input_array: MultiPointArray = vec![input_geom].into();
        let result_array = input_array.convex_hull();

        let expected = polygon![
            (x:0.0, y: -10.0),
            (x:10.0, y: 0.0),
            (x:0.0, y:10.0),
            (x:-10.0, y:0.0),
            (x:0.0, y:-10.0),
        ];

        assert_eq!(expected, result_array.get_as_geo(0).unwrap());
    }

    #[test]
    fn convex_hull_linestring_test() {
        let input_geom = line_string![
            (x: 0.0, y: 10.0),
            (x: 1.0, y: 1.0),
            (x: 10.0, y: 0.0),
            (x: 1.0, y: -1.0),
            (x: 0.0, y: -10.0),
            (x: -1.0, y: -1.0),
            (x: -10.0, y: 0.0),
            (x: -1.0, y: 1.0),
            (x: 0.0, y: 10.0),
        ];

        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = input_array.convex_hull();

        let expected = polygon![
            (x: 0.0, y: -10.0),
            (x: 10.0, y: 0.0),
            (x: 0.0, y: 10.0),
            (x: -10.0, y: 0.0),
            (x: 0.0, y: -10.0),
        ];

        assert_eq!(expected, result_array.get_as_geo(0).unwrap());
    }
}
