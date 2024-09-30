use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
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
    /// use geoarrow::trait_::ArrayAccessor;
    /// use geo::line_string;
    ///
    /// let line_string = line_string![
    ///     (x: 5.0, y: 2.0),
    ///     (x: 3.0, y: 8.0),
    ///     (x: 6.0, y: 20.0),
    ///     (x: 7.0, y: 25.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    /// let line_string_array: LineStringArray<2> = vec![line_string].as_slice().into();
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
        impl SimplifyVw for $type {
            type Output = Self;

            fn simplify_vw(&self, _epsilon: &f64) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(MultiPointArray<2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl SimplifyVw for $type {
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

iter_geo_impl!(LineStringArray<2>, geo::LineString);
iter_geo_impl!(PolygonArray<2>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<2>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<2>, geo::MultiPolygon);
// iter_geo_impl!(MixedGeometryArray<2>, geo::Geometry);
// iter_geo_impl!(GeometryCollectionArray<2>, geo::GeometryCollection);

impl SimplifyVw for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn simplify_vw(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().simplify_vw(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().simplify_vw(epsilon)),
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().simplify_vw(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().simplify_vw(epsilon)),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().simplify_vw(epsilon))
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().simplify_vw(epsilon)),
            // Mixed(_, XY) => self.as_mixed::<2>().simplify_vw(epsilon),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().simplify_vw(),
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
        impl SimplifyVw for $type {
            type Output = Self;

            fn simplify_vw(&self, epsilon: &f64) -> Self {
                self.map(|chunk| chunk.simplify_vw(epsilon))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray<2>>);
chunked_impl!(ChunkedGeometryArray<PolygonArray<2>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<2>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<2>>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray<2>>);

impl SimplifyVw for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn simplify_vw(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().simplify_vw(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().simplify_vw(epsilon)),
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().simplify_vw(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().simplify_vw(epsilon)),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().simplify_vw(epsilon))
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon::<2>().simplify_vw(epsilon)),
            // Mixed(_, XY) => self.as_mixed::<2>().simplify_vw(epsilon),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().simplify_vw(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
