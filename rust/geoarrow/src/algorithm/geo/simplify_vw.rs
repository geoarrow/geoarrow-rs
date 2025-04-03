use std::sync::Arc;

use crate::algorithm::broadcasting::BroadcastablePrimitive;
use crate::array::*;
use crate::chunked_array::{ChunkedGeometryArray, ChunkedNativeArray};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow::datatypes::Float64Type;
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
    /// use geoarrow::datatypes::Dimension;
    ///
    /// let line_string = line_string![
    ///     (x: 5.0, y: 2.0),
    ///     (x: 3.0, y: 8.0),
    ///     (x: 6.0, y: 20.0),
    ///     (x: 7.0, y: 25.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    /// let line_string_array: LineStringArray = (vec![line_string].as_slice(), Dimension::XY).into();
    ///
    /// let simplified_array = line_string_array.simplify_vw(&30.0.into());
    ///
    /// let expected = line_string![
    ///     (x: 5.0, y: 2.0),
    ///     (x: 7.0, y: 25.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    ///
    /// assert_eq!(expected, simplified_array.value_as_geo(0))
    /// ```
    fn simplify_vw(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output;
}

/// Implementation that returns the identity
macro_rules! identity_impl {
    ($type:ty) => {
        impl SimplifyVw for $type {
            type Output = Self;

            fn simplify_vw(&self, _epsilon: &BroadcastablePrimitive<Float64Type>) -> Self {
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
        impl SimplifyVw for $type {
            type Output = Self;

            fn simplify_vw(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(epsilon)
                    .map(|(maybe_g, epsilon)| {
                        if let (Some(geom), Some(eps)) = (maybe_g, epsilon) {
                            Some(geom.simplify_vw(&eps))
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

impl SimplifyVw for GeometryArray {
    type Output = Result<Self>;

    fn simplify_vw(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        let output_geoms: Vec<Option<geo::Geometry>> = self
            .iter_geo()
            .zip(epsilon)
            .map(|(maybe_g, epsilon)| {
                if let (Some(geom), Some(eps)) = (maybe_g, epsilon) {
                    let out = match geom {
                        geo::Geometry::LineString(g) => {
                            geo::Geometry::LineString(g.simplify_vw(&eps))
                        }
                        geo::Geometry::Polygon(g) => geo::Geometry::Polygon(g.simplify_vw(&eps)),
                        geo::Geometry::MultiLineString(g) => {
                            geo::Geometry::MultiLineString(g.simplify_vw(&eps))
                        }
                        geo::Geometry::MultiPolygon(g) => {
                            geo::Geometry::MultiPolygon(g.simplify_vw(&eps))
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

impl SimplifyVw for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn simplify_vw(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_) => Arc::new(self.as_point().simplify_vw(epsilon)),
            LineString(_) => Arc::new(self.as_line_string().simplify_vw(epsilon)),
            Polygon(_) => Arc::new(self.as_polygon().simplify_vw(epsilon)),
            MultiPoint(_) => Arc::new(self.as_multi_point().simplify_vw(epsilon)),
            MultiLineString(_) => Arc::new(self.as_multi_line_string().simplify_vw(epsilon)),
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().simplify_vw(epsilon)),
            Geometry(_) => Arc::new(self.as_geometry().simplify_vw(epsilon)?),
            // Mixed(_) => self.as_mixed().simplify_vw(epsilon),
            // GeometryCollection(_) => self.as_geometry_collection().simplify_vw(),
            _ => return Err(GeoArrowError::IncorrectType("simplify vw".into())),
        };
        Ok(result)
    }
}

impl SimplifyVw for ChunkedGeometryArray<PointArray> {
    type Output = Self;

    fn simplify_vw(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
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

            fn simplify_vw(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self {
                self.map(|chunk| chunk.simplify_vw(epsilon))
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

impl SimplifyVw for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn simplify_vw(&self, epsilon: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_) => Arc::new(self.as_point().simplify_vw(epsilon)),
            LineString(_) => Arc::new(self.as_line_string().simplify_vw(epsilon)),
            Polygon(_) => Arc::new(self.as_polygon().simplify_vw(epsilon)),
            MultiPoint(_) => Arc::new(self.as_multi_point().simplify_vw(epsilon)),
            MultiLineString(_) => Arc::new(self.as_multi_line_string().simplify_vw(epsilon)),
            MultiPolygon(_) => Arc::new(self.as_multi_polygon().simplify_vw(epsilon)),
            // Mixed(_) => self.as_mixed().simplify_vw(epsilon),
            // GeometryCollection(_) => self.as_geometry_collection().simplify_vw(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
