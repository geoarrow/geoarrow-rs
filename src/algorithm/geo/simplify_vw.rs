use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedGeometryArrayTrait};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geo::SimplifyVw as _SimplifyVw;

/// Simplifies a geometry.
///
/// Polygons are simplified by running the algorithm on all their constituent rings.  This may
/// result in invalid Polygons, and has no guarantee of preserving topology. Multi* objects are
/// simplified by simplifying all their constituent geometries individually.
///
/// An epsilon less than or equal to zero will return an unaltered version of the geometry.
pub trait SimplifyVw {
    type Output;

    /// Returns the simplified representation of a geometry, using the [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263) algorithm
    ///
    /// See [here](https://bost.ocks.org/mike/simplify/) for a graphical explanation
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::algorithm::geo::SimplifyVw;
    /// use geoarrow::array::LineStringArray;
    /// use geoarrow::trait_::GeometryArrayAccessor;
    /// use geo::line_string;
    ///
    /// let line_string = line_string![
    ///     (x: 5.0, y: 2.0),
    ///     (x: 3.0, y: 8.0),
    ///     (x: 6.0, y: 20.0),
    ///     (x: 7.0, y: 25.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    /// let line_string_array: LineStringArray<i32, 2> = vec![line_string].as_slice().into();
    ///
    /// let simplified_array = line_string_array.simplify_vw(&30.0);
    ///
    /// let expected = line_string![
    ///     (x: 5.0, y: 2.0),
    ///     (x: 7.0, y: 25.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    ///
    /// assert_eq!(expected, simplified_array.value_as_geo(0))
    /// ```
    fn simplify_vw(&self, epsilon: &f64) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl SimplifyVw for PointArray<2> {
    type Output = Self;

    fn simplify_vw(&self, _epsilon: &f64) -> Self {
        self.clone()
    }
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVw for $type {
            type Output = Self;

            fn simplify_vw(&self, _epsilon: &f64) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(MultiPointArray<O, 2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVw for $type {
            type Output = Self;

            fn simplify_vw(&self, epsilon: &f64) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.simplify_vw(epsilon)))
                    .collect();

                output_geoms.into()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O, 2>, geo::LineString);
iter_geo_impl!(PolygonArray<O, 2>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<O, 2>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<O, 2>, geo::MultiPolygon);
// iter_geo_impl!(MixedGeometryArray<O, 2>, geo::Geometry);
// iter_geo_impl!(GeometryCollectionArray<O, 2>, geo::GeometryCollection);

impl SimplifyVw for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn simplify_vw(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().simplify_vw(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().simplify_vw(epsilon)),
            LargeLineString(_, XY) => {
                Arc::new(self.as_large_line_string::<2>().simplify_vw(epsilon))
            }
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().simplify_vw(epsilon)),
            LargePolygon(_, XY) => Arc::new(self.as_large_polygon::<2>().simplify_vw(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().simplify_vw(epsilon)),
            LargeMultiPoint(_, XY) => {
                Arc::new(self.as_large_multi_point::<2>().simplify_vw(epsilon))
            }
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().simplify_vw(epsilon))
            }
            LargeMultiLineString(_, XY) => {
                Arc::new(self.as_large_multi_line_string::<2>().simplify_vw(epsilon))
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().simplify_vw(epsilon)),
            LargeMultiPolygon(_, XY) => {
                Arc::new(self.as_large_multi_polygon::<2>().simplify_vw(epsilon))
            }
            // Mixed(_, XY) => self.as_mixed::<2>().simplify_vw(epsilon),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().simplify_vw(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().simplify_vw(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().simplify_vw()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl SimplifyVw for ChunkedGeometryArray<PointArray<2>> {
    type Output = Self;

    fn simplify_vw(&self, epsilon: &f64) -> Self::Output {
        self.map(|chunk| chunk.simplify_vw(epsilon))
            .try_into()
            .unwrap()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVw for $type {
            type Output = Self;

            fn simplify_vw(&self, epsilon: &f64) -> Self {
                self.map(|chunk| chunk.simplify_vw(epsilon))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<PolygonArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<O, 2>>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray<O, 2>>);

impl SimplifyVw for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

    fn simplify_vw(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().simplify_vw(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().simplify_vw(epsilon)),
            LargeLineString(_, XY) => {
                Arc::new(self.as_large_line_string::<2>().simplify_vw(epsilon))
            }
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().simplify_vw(epsilon)),
            LargePolygon(_, XY) => Arc::new(self.as_large_polygon::<2>().simplify_vw(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().simplify_vw(epsilon)),
            LargeMultiPoint(_, XY) => {
                Arc::new(self.as_large_multi_point::<2>().simplify_vw(epsilon))
            }
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().simplify_vw(epsilon))
            }
            LargeMultiLineString(_, XY) => {
                Arc::new(self.as_large_multi_line_string::<2>().simplify_vw(epsilon))
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().simplify_vw(epsilon)),
            LargeMultiPolygon(_, XY) => {
                Arc::new(self.as_large_multi_polygon::<2>().simplify_vw(epsilon))
            }
            // Mixed(_, XY) => self.as_mixed::<2>().simplify_vw(epsilon),
            // LargeMixed(_, XY) => self.as_large_mixed::<2>().simplify_vw(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().simplify_vw(),
            // LargeGeometryCollection(_, XY) => {
            //     self.as_large_geometry_collection::<2>().simplify_vw()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
