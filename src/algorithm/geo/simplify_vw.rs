use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::ChunkedGeometryArray;
use crate::datatypes::GeoDataType;
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
    /// let line_string_array: LineStringArray<i32> = vec![line_string].as_slice().into();
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
impl SimplifyVw for PointArray {
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

identity_impl!(MultiPointArray<O>);

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

iter_geo_impl!(LineStringArray<O>, geo::LineString);
iter_geo_impl!(PolygonArray<O>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<O>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<O>, geo::MultiPolygon);
// iter_geo_impl!(MixedGeometryArray<O>, geo::Geometry);
// iter_geo_impl!(GeometryCollectionArray<O>, geo::GeometryCollection);

impl SimplifyVw for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn simplify_vw(&self, epsilon: &f64) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().simplify_vw(epsilon)),
            GeoDataType::LineString(_) => Arc::new(self.as_line_string().simplify_vw(epsilon)),
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().simplify_vw(epsilon))
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().simplify_vw(epsilon)),
            GeoDataType::LargePolygon(_) => Arc::new(self.as_large_polygon().simplify_vw(epsilon)),
            GeoDataType::MultiPoint(_) => Arc::new(self.as_multi_point().simplify_vw(epsilon)),
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().simplify_vw(epsilon))
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().simplify_vw(epsilon))
            }
            GeoDataType::LargeMultiLineString(_) => {
                Arc::new(self.as_large_multi_line_string().simplify_vw(epsilon))
            }
            GeoDataType::MultiPolygon(_) => Arc::new(self.as_multi_polygon().simplify_vw(epsilon)),
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().simplify_vw(epsilon))
            }
            // GeoDataType::Mixed(_) => self.as_mixed().simplify_vw(epsilon),
            // GeoDataType::LargeMixed(_) => self.as_large_mixed().simplify_vw(),
            // GeoDataType::GeometryCollection(_) => self.as_geometry_collection().simplify_vw(),
            // GeoDataType::LargeGeometryCollection(_) => {
            //     self.as_large_geometry_collection().simplify_vw()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl SimplifyVw for ChunkedGeometryArray<PointArray> {
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

chunked_impl!(ChunkedGeometryArray<LineStringArray<O>>);
chunked_impl!(ChunkedGeometryArray<PolygonArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray<O>>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray<O>>);
