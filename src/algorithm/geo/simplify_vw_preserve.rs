use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
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
    ///   points remaining (3 for a `LineString`, 5 for a `Polygon`), the point is retained and the
    ///   simplification process ends. This is because there is no guarantee that removal of two
    ///   points will remove the intersection, but removal of further points would leave too few
    ///   points to form a valid geometry.
    /// - The tolerance used to remove a point is `epsilon`, in keeping with GEOS. JTS uses
    ///   `epsilon ^ 2`
    fn simplify_vw_preserve(&self, epsilon: &f64) -> Self::Output;
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl SimplifyVwPreserve for $type {
            type Output = Self;

            fn simplify_vw_preserve(&self, _epsilon: &f64) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(PointArray<2>);
identity_impl!(MultiPointArray<2>);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $geo_type:ty) => {
        impl SimplifyVwPreserve for $type {
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

iter_geo_impl!(LineStringArray<2>, geo::LineString);
iter_geo_impl!(PolygonArray<2>, geo::Polygon);
iter_geo_impl!(MultiLineStringArray<2>, geo::MultiLineString);
iter_geo_impl!(MultiPolygonArray<2>, geo::MultiPolygon);
// iter_geo_impl!(MixedGeometryArray<2>, geo::Geometry);
// iter_geo_impl!(GeometryCollectionArray<2>, geo::GeometryCollection);

impl SimplifyVwPreserve for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn simplify_vw_preserve(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().simplify_vw_preserve(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().simplify_vw_preserve(epsilon)),
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().simplify_vw_preserve(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().simplify_vw_preserve(epsilon)),
            MultiLineString(_, XY) => Arc::new(
                self.as_multi_line_string::<2>()
                    .simplify_vw_preserve(epsilon),
            ),
            MultiPolygon(_, XY) => {
                Arc::new(self.as_multi_polygon::<2>().simplify_vw_preserve(epsilon))
            }
            // Mixed(_, XY) => self.as_mixed::<2>().simplify_vw_preserve(epsilon),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().simplify_vw_preserve(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl SimplifyVwPreserve for ChunkedGeometryArray<PointArray<2>> {
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
        impl SimplifyVwPreserve for $type {
            type Output = Self;

            fn simplify_vw_preserve(&self, epsilon: &f64) -> Self {
                self.map(|chunk| chunk.simplify_vw_preserve(epsilon))
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

impl SimplifyVwPreserve for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn simplify_vw_preserve(&self, epsilon: &f64) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().simplify_vw_preserve(epsilon)),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().simplify_vw_preserve(epsilon)),
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().simplify_vw_preserve(epsilon)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().simplify_vw_preserve(epsilon)),
            MultiLineString(_, XY) => Arc::new(
                self.as_multi_line_string::<2>()
                    .simplify_vw_preserve(epsilon),
            ),
            MultiPolygon(_, XY) => {
                Arc::new(self.as_multi_polygon::<2>().simplify_vw_preserve(epsilon))
            }
            // Mixed(_, XY) => self.as_mixed::<2>().simplify_vw_preserve(epsilon),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().simplify_vw_preserve(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
