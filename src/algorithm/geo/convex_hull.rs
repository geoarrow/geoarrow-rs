use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedPolygonArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::OffsetSizeTrait;
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
    type Output;

    fn convex_hull(&self) -> Self::Output;
}

impl ConvexHull for PointArray<2> {
    type Output = PolygonArray<2>;

    fn convex_hull(&self) -> Self::Output {
        let output_geoms: Vec<Option<Polygon>> = self.iter_geo().map(|maybe_g| maybe_g.map(|geom| geom.convex_hull())).collect();

        output_geoms.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl ConvexHull for $type {
            type Output = PolygonArray<2>;

            fn convex_hull(&self) -> Self::Output {
                let output_geoms: Vec<Option<Polygon>> = self.iter_geo().map(|maybe_g| maybe_g.map(|geom| geom.convex_hull())).collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);
iter_geo_impl!(MixedGeometryArray<2>);
iter_geo_impl!(GeometryCollectionArray<2>);

impl ConvexHull for &dyn NativeArray {
    type Output = Result<PolygonArray<2>>;

    fn convex_hull(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result = match self.data_type() {
            Point(_, XY) => self.as_point::<2>().convex_hull(),
            LineString(_, XY) => self.as_line_string::<2>().convex_hull(),
            Polygon(_, XY) => self.as_polygon::<2>().convex_hull(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().convex_hull(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().convex_hull(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().convex_hull(),
            Mixed(_, XY) => self.as_mixed::<2>().convex_hull(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().convex_hull(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> ConvexHull for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<PolygonArray<2>>>;

    fn convex_hull(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().convex_hull())?.try_into()
    }
}

impl ConvexHull for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedPolygonArray<2>>;

    fn convex_hull(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().convex_hull(),
            LineString(_, XY) => self.as_line_string::<2>().convex_hull(),
            Polygon(_, XY) => self.as_polygon::<2>().convex_hull(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().convex_hull(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().convex_hull(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().convex_hull(),
            Mixed(_, XY) => self.as_mixed::<2>().convex_hull(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().convex_hull(),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConvexHull;
    use crate::array::polygon::PolygonArray;
    use crate::array::{LineStringArray, MultiPointArray};
    use crate::trait_::ArrayAccessor;
    use geo::{line_string, polygon, MultiPoint, Point};

    #[test]
    fn convex_hull_for_multipoint() {
        // Values borrowed from this test in geo crate: https://docs.rs/geo/0.14.2/src/geo/algorithm/convexhull.rs.html#323
        let input_geom: MultiPoint = vec![Point::new(0.0, 10.0), Point::new(1.0, 1.0), Point::new(10.0, 0.0), Point::new(1.0, -1.0), Point::new(0.0, -10.0), Point::new(-1.0, -1.0), Point::new(-10.0, 0.0), Point::new(-1.0, 1.0), Point::new(0.0, 10.0)].into();
        let input_array: MultiPointArray<2> = vec![input_geom].as_slice().into();
        let result_array: PolygonArray<2> = input_array.convex_hull();

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

        let input_array: LineStringArray<2> = vec![input_geom].as_slice().into();
        let result_array: PolygonArray<2> = input_array.convex_hull();

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
