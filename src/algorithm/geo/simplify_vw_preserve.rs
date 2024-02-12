use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedGeometryArrayTrait};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use geo::SimplifyVwPreserve as _SimplifyVwPreserve;

/// Simplifies a geometry, attempting to preserve its topology by removing self-intersections
///
/// An epsilon less than or equal to zero will return an unaltered version of the geometry
pub trait SimplifyVwPreserve {
    type Output;

    /// Returns the simplified representation of a geometry, using a topology-preserving variant of
    /// the [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263)
    /// algorithm.
    ///
    /// See [here](https://www.jasondavies.com/simplify/) for a graphical explanation.
    ///
    /// The topology-preserving algorithm uses an [R* tree](../../../rstar/struct.RTree.html) to
    /// efficiently find candidate line segments which are tested for intersection with a given
    /// triangle. If intersections are found, the previous point (i.e. the left component of the
    /// current triangle) is also removed, altering the geometry and removing the intersection.
    ///
    /// In the example below, `(135.0, 68.0)` would be retained by the standard algorithm, forming
    /// triangle `(0, 1, 3),` which intersects with the segments `(280.0, 19.0), (117.0, 48.0)` and
    /// `(117.0, 48.0), (300,0, 40.0)`. By removing it, a new triangle with indices `(0, 3, 4)` is
    /// formed, which does not cause a self-intersection.
    ///
    /// # Notes
    ///
    /// - It is possible for the simplification algorithm to displace a Polygon's interior ring
    ///   outside its shell.
    /// - The algorithm does **not** guarantee a valid output geometry, especially on smaller
    ///   geometries.
    /// - If removal of a point causes a self-intersection, but the geometry only has `n + 1`
    /// points remaining (3 for a `LineString`, 5 for a `Polygon`), the point is retained and the
    /// simplification process ends. This is because there is no guarantee that removal of two
    /// points will remove the intersection, but removal of further points would leave too few
    /// points to form a valid geometry.
    /// - The tolerance used to remove a point is `epsilon`, in keeping with GEOS. JTS uses
    ///   `epsilon ^ 2`
    fn simplify_vw_preserve(&self, epsilon: &f64) -> Self::Output;
}

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl SimplifyVwPreserve for PointArray {
    type Output = Self;

    fn simplify_vw_preserve(&self, _epsilon: &f64) -> Self {
        self.clone()
    }
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVwPreserve for $type {
            type Output = Self;

            fn simplify_vw_preserve(&self, _epsilon: &f64) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(MultiPointArray<O>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVwPreserve for $type {
            type Output = Self;

            fn simplify_vw_preserve(&self, epsilon: &f64) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| maybe_g.map(|geom| geom.simplify_vw_preserve(epsilon)))
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

impl SimplifyVwPreserve for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn simplify_vw_preserve(&self, epsilon: &f64) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().simplify_vw_preserve(epsilon)),
            GeoDataType::LineString(_) => {
                Arc::new(self.as_line_string().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().simplify_vw_preserve(epsilon))
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().simplify_vw_preserve(epsilon)),
            GeoDataType::LargePolygon(_) => {
                Arc::new(self.as_large_polygon().simplify_vw_preserve(epsilon))
            }
            GeoDataType::MultiPoint(_) => {
                Arc::new(self.as_multi_point().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().simplify_vw_preserve(epsilon))
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeMultiLineString(_) => Arc::new(
                self.as_large_multi_line_string()
                    .simplify_vw_preserve(epsilon),
            ),
            GeoDataType::MultiPolygon(_) => {
                Arc::new(self.as_multi_polygon().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().simplify_vw_preserve(epsilon))
            }
            // GeoDataType::Mixed(_) => self.as_mixed().simplify_vw_preserve(epsilon),
            // GeoDataType::LargeMixed(_) => self.as_large_mixed().simplify_vw_preserve(),
            // GeoDataType::GeometryCollection(_) => self.as_geometry_collection().simplify_vw_preserve(),
            // GeoDataType::LargeGeometryCollection(_) => {
            //     self.as_large_geometry_collection().simplify_vw_preserve()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl SimplifyVwPreserve for ChunkedGeometryArray<PointArray> {
    type Output = Self;

    fn simplify_vw_preserve(&self, epsilon: &f64) -> Self::Output {
        self.map(|chunk| chunk.simplify_vw_preserve(epsilon))
            .try_into()
            .unwrap()
    }
}

/// Implementation that iterates over chunks
macro_rules! chunked_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> SimplifyVwPreserve for $type {
            type Output = Self;

            fn simplify_vw_preserve(&self, epsilon: &f64) -> Self {
                self.map(|chunk| chunk.simplify_vw_preserve(epsilon))
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

impl SimplifyVwPreserve for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

    fn simplify_vw_preserve(&self, epsilon: &f64) -> Self::Output {
        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().simplify_vw_preserve(epsilon)),
            GeoDataType::LineString(_) => {
                Arc::new(self.as_line_string().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().simplify_vw_preserve(epsilon))
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().simplify_vw_preserve(epsilon)),
            GeoDataType::LargePolygon(_) => {
                Arc::new(self.as_large_polygon().simplify_vw_preserve(epsilon))
            }
            GeoDataType::MultiPoint(_) => {
                Arc::new(self.as_multi_point().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().simplify_vw_preserve(epsilon))
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeMultiLineString(_) => Arc::new(
                self.as_large_multi_line_string()
                    .simplify_vw_preserve(epsilon),
            ),
            GeoDataType::MultiPolygon(_) => {
                Arc::new(self.as_multi_polygon().simplify_vw_preserve(epsilon))
            }
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().simplify_vw_preserve(epsilon))
            }
            // GeoDataType::Mixed(_) => self.as_mixed().simplify_vw_preserve(epsilon),
            // GeoDataType::LargeMixed(_) => self.as_large_mixed().simplify_vw_preserve(),
            // GeoDataType::GeometryCollection(_) => self.as_geometry_collection().simplify_vw_preserve(),
            // GeoDataType::LargeGeometryCollection(_) => {
            //     self.as_large_geometry_collection().simplify_vw_preserve()
            // }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
