use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray, ChunkedPolygonArray};
use crate::datatypes::NativeType;
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::algorithm::convex_hull::ConvexHull as GeoConvexHull;
use geo::Polygon;
use geoarrow_schema::Dimension;

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

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl ConvexHull for $type {
            type Output = PolygonArray;

            fn convex_hull(&self) -> Self::Output {
                let output_geoms: Vec<Option<Polygon>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                    .collect();

                PolygonBuilder::from_nullable_polygons(
                    output_geoms.as_slice(),
                    Dimension::XY,
                    self.coord_type(),
                    self.metadata().clone(),
                )
                .finish()
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
iter_geo_impl!(MixedGeometryArray);
iter_geo_impl!(GeometryCollectionArray);
iter_geo_impl!(RectArray);
iter_geo_impl!(GeometryArray);

impl ConvexHull for &dyn NativeArray {
    type Output = Result<PolygonArray>;

    fn convex_hull(&self) -> Self::Output {
        use NativeType::*;

        let result = match self.data_type() {
            Point(_) => self.as_point().convex_hull(),
            LineString(_) => self.as_line_string().convex_hull(),
            Polygon(_) => self.as_polygon().convex_hull(),
            MultiPoint(_) => self.as_multi_point().convex_hull(),
            MultiLineString(_) => self.as_multi_line_string().convex_hull(),
            MultiPolygon(_) => self.as_multi_polygon().convex_hull(),
            GeometryCollection(_) => self.as_geometry_collection().convex_hull(),
            Rect(_) => self.as_rect().convex_hull(),
            Geometry(_) => self.as_geometry().convex_hull(),
        };
        Ok(result)
    }
}

impl<G: NativeArray> ConvexHull for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<PolygonArray>>;

    fn convex_hull(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().convex_hull())?
            .try_into()
    }
}

impl ConvexHull for &dyn ChunkedNativeArray {
    type Output = Result<ChunkedPolygonArray>;

    fn convex_hull(&self) -> Self::Output {
        use NativeType::*;

        match self.data_type() {
            Point(_) => self.as_point().convex_hull(),
            LineString(_) => self.as_line_string().convex_hull(),
            Polygon(_) => self.as_polygon().convex_hull(),
            MultiPoint(_) => self.as_multi_point().convex_hull(),
            MultiLineString(_) => self.as_multi_line_string().convex_hull(),
            MultiPolygon(_) => self.as_multi_polygon().convex_hull(),
            GeometryCollection(_) => self.as_geometry_collection().convex_hull(),
            Rect(_) => self.as_rect().convex_hull(),
            Geometry(_) => self.as_geometry().convex_hull(),
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
    use geoarrow_schema::Dimension;

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
        let input_array: MultiPointArray = (vec![input_geom].as_slice(), Dimension::XY).into();
        let result_array: PolygonArray = input_array.convex_hull();

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

        let input_array: LineStringArray = (vec![input_geom].as_slice(), Dimension::XY).into();
        let result_array: PolygonArray = input_array.convex_hull();

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
