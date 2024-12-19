use std::sync::Arc;

use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow::datatypes::Float64Type;
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
    fn simplify_vw_preserve(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output;
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl SimplifyVwPreserve for $type {
            type Output = Self;

            fn simplify_vw_preserve(&self, _epsilon: &BroadcastablePrimitive<Float64Type>) -> Self {
                self.clone()
            }
        }
    };
}

identity_impl!(PointArray);
identity_impl!(MultiPointArray);

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $method:ident, $geo_type:ty) => {
        impl SimplifyVwPreserve for $type {
            type Output = Self;

            fn simplify_vw_preserve(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(epsilon)
                    .map(|(maybe_g, epsilon)| {
                        if let (Some(geom), Some(eps)) = (maybe_g, epsilon) {
                            Some(geom.simplify_vw_preserve(&eps))
                        } else {
                            None
                        }
                    })
                    .collect();

                <$builder_type>::$method(
                    output_geoms.as_slice(),
                    Dimension::XY,
                    self.coord_type(),
                    self.metadata.clone(),
                )
                .finish()
            }
        }
    };
}

iter_geo_impl!(
    LineStringArray,
    LineStringBuilder,
    from_nullable_line_strings,
    geo::LineString
);
iter_geo_impl!(
    PolygonArray,
    PolygonBuilder,
    from_nullable_polygons,
    geo::Polygon
);
iter_geo_impl!(
    MultiLineStringArray,
    MultiLineStringBuilder,
    from_nullable_multi_line_strings,
    geo::MultiLineString
);
iter_geo_impl!(
    MultiPolygonArray,
    MultiPolygonBuilder,
    from_nullable_multi_polygons,
    geo::MultiPolygon
);

impl SimplifyVwPreserve for GeometryArray {
    type Output = Result<Self>;

    fn simplify_vw_preserve(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        let output_geoms: Vec<Option<geo::Geometry>> = self
            .iter_geo()
            .zip(epsilon)
            .map(|(maybe_g, epsilon)| {
                if let (Some(geom), Some(eps)) = (maybe_g, epsilon) {
                    let out = match geom {
                        geo::Geometry::LineString(g) => {
                            geo::Geometry::LineString(g.simplify_vw_preserve(&eps))
                        }
                        geo::Geometry::Polygon(g) => {
                            geo::Geometry::Polygon(g.simplify_vw_preserve(&eps))
                        }
                        geo::Geometry::MultiLineString(g) => {
                            geo::Geometry::MultiLineString(g.simplify_vw_preserve(&eps))
                        }
                        geo::Geometry::MultiPolygon(g) => {
                            geo::Geometry::MultiPolygon(g.simplify_vw_preserve(&eps))
                        }
                        g => g,
                    };
                    Some(out)
                } else {
                    None
                }
            })
            .collect();

        let builder = GeometryBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            self.coord_type(),
            self.metadata().clone(),
            false,
        )?;
        Ok(builder.finish())
    }
}

impl SimplifyVwPreserve for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn simplify_vw_preserve(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, _) => Arc::new(self.as_point().simplify_vw_preserve(epsilon)),
            LineString(_, _) => Arc::new(self.as_line_string().simplify_vw_preserve(epsilon)),
            Polygon(_, _) => Arc::new(self.as_polygon().simplify_vw_preserve(epsilon)),
            MultiPoint(_, _) => Arc::new(self.as_multi_point().simplify_vw_preserve(epsilon)),
            MultiLineString(_, _) => {
                Arc::new(self.as_multi_line_string().simplify_vw_preserve(epsilon))
            }
            MultiPolygon(_, _) => Arc::new(self.as_multi_polygon().simplify_vw_preserve(epsilon)),
            Geometry(_) => Arc::new(self.as_geometry().simplify_vw_preserve(epsilon)?),
            // Mixed(_, _) => self.as_mixed().simplify_vw_preserve(epsilon),
            // GeometryCollection(_, _) => self.as_geometry_collection().simplify_vw_preserve(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl SimplifyVwPreserve for ChunkedGeometryArray<PointArray> {
    type Output = Self;

    fn simplify_vw_preserve(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
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

            fn simplify_vw_preserve(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self {
                self.map(|chunk| chunk.simplify_vw_preserve(epsilon))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

chunked_impl!(ChunkedGeometryArray<LineStringArray>);
chunked_impl!(ChunkedGeometryArray<PolygonArray>);
chunked_impl!(ChunkedGeometryArray<MultiPointArray>);
chunked_impl!(ChunkedGeometryArray<MultiLineStringArray>);
chunked_impl!(ChunkedGeometryArray<MultiPolygonArray>);

impl SimplifyVwPreserve for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn simplify_vw_preserve(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, _) => Arc::new(self.as_point().simplify_vw_preserve(epsilon)),
            LineString(_, _) => Arc::new(self.as_line_string().simplify_vw_preserve(epsilon)),
            Polygon(_, _) => Arc::new(self.as_polygon().simplify_vw_preserve(epsilon)),
            MultiPoint(_, _) => Arc::new(self.as_multi_point().simplify_vw_preserve(epsilon)),
            MultiLineString(_, _) => {
                Arc::new(self.as_multi_line_string().simplify_vw_preserve(epsilon))
            }
            MultiPolygon(_, _) => Arc::new(self.as_multi_polygon().simplify_vw_preserve(epsilon)),
            // Mixed(_, _) => self.as_mixed().simplify_vw_preserve(epsilon),
            // GeometryCollection(_, _) => self.as_geometry_collection().simplify_vw_preserve(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
