use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
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
pub trait ConvexHull<O: OffsetSizeTrait> {
    type Output;

    fn convex_hull(&self) -> Self::Output;
}

impl<O: OffsetSizeTrait> ConvexHull<O> for PointArray {
    type Output = PolygonArray<O>;

    fn convex_hull(&self) -> Self::Output {
        let output_geoms: Vec<Option<Polygon>> = self
            .iter_geo()
            .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
            .collect();

        output_geoms.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait, O2: OffsetSizeTrait> ConvexHull<O> for $type {
            type Output = PolygonArray<O>;

            fn convex_hull(&self) -> Self::Output {
                let output_geoms: Vec<Option<Polygon>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O2>);
iter_geo_impl!(PolygonArray<O2>);
iter_geo_impl!(MultiPointArray<O2>);
iter_geo_impl!(MultiLineStringArray<O2>);
iter_geo_impl!(MultiPolygonArray<O2>);
iter_geo_impl!(MixedGeometryArray<O2>);
iter_geo_impl!(GeometryCollectionArray<O2>);
iter_geo_impl!(WKBArray<O2>);

impl<O: OffsetSizeTrait> ConvexHull<O> for &dyn GeometryArrayTrait {
    type Output = Result<PolygonArray<O>>;

    fn convex_hull(&self) -> Self::Output {
        let result = match self.data_type() {
            GeoDataType::Point(_) => self.as_point().convex_hull(),
            GeoDataType::LineString(_) => self.as_line_string().convex_hull(),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().convex_hull(),
            GeoDataType::Polygon(_) => self.as_polygon().convex_hull(),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().convex_hull(),
            GeoDataType::MultiPoint(_) => self.as_multi_point().convex_hull(),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().convex_hull(),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().convex_hull(),
            GeoDataType::LargeMultiLineString(_) => self.as_large_multi_line_string().convex_hull(),
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().convex_hull(),
            GeoDataType::LargeMultiPolygon(_) => self.as_large_multi_polygon().convex_hull(),
            GeoDataType::Mixed(_) => self.as_mixed().convex_hull(),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().convex_hull(),
            GeoDataType::GeometryCollection(_) => self.as_geometry_collection().convex_hull(),
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().convex_hull()
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<O: OffsetSizeTrait, G: GeometryArrayTrait> ConvexHull<O> for ChunkedGeometryArray<G> {
    type Output = Result<ChunkedGeometryArray<PolygonArray<O>>>;

    fn convex_hull(&self) -> Self::Output {
        self.try_map(|chunk| chunk.as_ref().convex_hull())?
            .try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::ConvexHull;
    use crate::array::polygon::PolygonArray;
    use crate::array::{LineStringArray, MultiPointArray};
    use crate::trait_::GeometryArrayAccessor;
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
        let input_array: MultiPointArray<i64> = vec![input_geom].as_slice().into();
        let result_array: PolygonArray<i32> = input_array.convex_hull();

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

        let input_array: LineStringArray<i64> = vec![input_geom].as_slice().into();
        let result_array: PolygonArray<i32> = input_array.convex_hull();

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
