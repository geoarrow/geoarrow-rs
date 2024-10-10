use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::{AffineTransform, MapCoords};

/// Apply an [`AffineTransform`] like [`scale`](AffineTransform::scale),
/// [`skew`](AffineTransform::skew), or [`rotate`](AffineTransform::rotate) to geometries.
///
/// Multiple transformations can be composed in order to be efficiently applied in a single
/// operation. See [`AffineTransform`] for more on how to build up a transformation.
///
/// If you are not composing operations, traits that leverage this same machinery exist which might
/// be more readable. See: [`Scale`](crate::algorithm::geo::Scale),
/// [`Translate`](crate::algorithm::geo::Translate), [`Rotate`](crate::algorithm::geo::Rotate), and
/// [`Skew`](crate::algorithm::geo::Skew).
///
/// # Examples
/// ## Build up transforms by beginning with a constructor, then chaining mutation operations
/// ```
/// use geo::{AffineOps, AffineTransform};
/// use geo::{line_string, BoundingRect, Point, LineString};
/// use approx::assert_relative_eq;
///
/// let ls: LineString = line_string![
///     (x: 0.0f64, y: 0.0f64),
///     (x: 0.0f64, y: 10.0f64),
/// ];
/// let center = ls.bounding_rect().unwrap().center();
///
/// let transform = AffineTransform::skew(40.0, 40.0, center).rotated(45.0, center);
///
/// let skewed_rotated = ls.affine_transform(&transform);
///
/// assert_relative_eq!(skewed_rotated, line_string![
///     (x: 0.5688687f64, y: 4.4311312),
///     (x: -0.5688687, y: 5.5688687)
/// ], max_relative = 1.0);
/// ```
pub trait AffineOps<Rhs> {
    type Output;

    /// Apply `transform` immutably, outputting a new geometry.
    #[must_use]
    fn affine_transform(&self, transform: Rhs) -> Self::Output;
}

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl AffineOps<&AffineTransform> for PointArray<2> {
    type Output = Self;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo().for_each(|maybe_g| {
            output_array.push_point(
                maybe_g
                    .map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                    .as_ref(),
            )
        });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl AffineOps<&AffineTransform> for $type {
            type Output = Self;

            fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo().for_each(|maybe_g| {
                    output_array
                        .$push_func(
                            maybe_g
                                .map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                                .as_ref(),
                        )
                        .unwrap()
                });

                output_array.finish()
            }
        }
    };
}

iter_geo_impl!(LineStringArray<2>, LineStringBuilder<2>, push_line_string);
iter_geo_impl!(PolygonArray<2>, PolygonBuilder<2>, push_polygon);
iter_geo_impl!(MultiPointArray<2>, MultiPointBuilder<2>, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray<2>,
    MultiLineStringBuilder<2>,
    push_multi_line_string
);
iter_geo_impl!(
    MultiPolygonArray<2>,
    MultiPolygonBuilder<2>,
    push_multi_polygon
);
iter_geo_impl!(
    MixedGeometryArray<2>,
    MixedGeometryBuilder<2>,
    push_geometry
);
iter_geo_impl!(
    GeometryCollectionArray<2>,
    GeometryCollectionBuilder<2>,
    push_geometry_collection
);

impl AffineOps<&AffineTransform> for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        macro_rules! impl_downcast {
            ($method:ident) => {
                Arc::new(self.$method().affine_transform(transform))
            };
        }
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_downcast!(as_point),
            LineString(_, XY) => impl_downcast!(as_line_string),
            Polygon(_, XY) => impl_downcast!(as_polygon),
            MultiPoint(_, XY) => impl_downcast!(as_multi_point),
            MultiLineString(_, XY) => impl_downcast!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_downcast!(as_multi_polygon),
            Mixed(_, XY) => impl_downcast!(as_mixed),
            GeometryCollection(_, XY) => impl_downcast!(as_geometry_collection),
            // Rect => impl_downcast!(as_rect),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl AffineOps<&AffineTransform> for $struct_name {
            type Output = Self;

            fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
                self.map(|chunk| chunk.affine_transform(transform))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedPointArray<2>);
impl_chunked!(ChunkedLineStringArray<2>);
impl_chunked!(ChunkedPolygonArray<2>);
impl_chunked!(ChunkedMultiPointArray<2>);
impl_chunked!(ChunkedMultiLineStringArray<2>);
impl_chunked!(ChunkedMultiPolygonArray<2>);
impl_chunked!(ChunkedMixedGeometryArray<2>);
impl_chunked!(ChunkedGeometryCollectionArray<2>);

impl AffineOps<&AffineTransform> for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        macro_rules! impl_downcast {
            ($method:ident) => {
                Arc::new(self.$method().affine_transform(transform))
            };
        }
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, XY) => impl_downcast!(as_point),
            LineString(_, XY) => impl_downcast!(as_line_string),
            Polygon(_, XY) => impl_downcast!(as_polygon),
            MultiPoint(_, XY) => impl_downcast!(as_multi_point),
            MultiLineString(_, XY) => impl_downcast!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_downcast!(as_multi_polygon),
            Mixed(_, XY) => impl_downcast!(as_mixed),
            GeometryCollection(_, XY) => impl_downcast!(as_geometry_collection),
            // Rect => impl_downcast!(as_rect),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl AffineOps<&[AffineTransform]> for PointArray<2> {
    type Output = Self;

    fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
        let mut output_array = PointBuilder::with_capacity(self.buffer_lengths());

        self.iter_geo()
            .zip(transform.iter())
            .for_each(|(maybe_g, transform)| {
                output_array.push_point(
                    maybe_g
                        .map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                        .as_ref(),
                )
            });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl2 {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl AffineOps<&[AffineTransform]> for $type {
            type Output = Self;

            fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
                let mut output_array = <$builder_type>::with_capacity(self.buffer_lengths());

                self.iter_geo()
                    .zip(transform.iter())
                    .for_each(|(maybe_g, transform)| {
                        output_array
                            .$push_func(
                                maybe_g
                                    .map(|geom| geom.map_coords(|coord| transform.apply(coord)))
                                    .as_ref(),
                            )
                            .unwrap()
                    });

                output_array.finish()
            }
        }
    };
}

iter_geo_impl2!(LineStringArray<2>, LineStringBuilder<2>, push_line_string);
iter_geo_impl2!(PolygonArray<2>, PolygonBuilder<2>, push_polygon);
iter_geo_impl2!(MultiPointArray<2>, MultiPointBuilder<2>, push_multi_point);
iter_geo_impl2!(
    MultiLineStringArray<2>,
    MultiLineStringBuilder<2>,
    push_multi_line_string
);
iter_geo_impl2!(
    MultiPolygonArray<2>,
    MultiPolygonBuilder<2>,
    push_multi_polygon
);
iter_geo_impl2!(
    MixedGeometryArray<2>,
    MixedGeometryBuilder<2>,
    push_geometry
);
iter_geo_impl2!(
    GeometryCollectionArray<2>,
    GeometryCollectionBuilder<2>,
    push_geometry_collection
);

impl AffineOps<&[AffineTransform]> for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point::<2>().affine_transform(transform)),
            LineString(_, XY) => Arc::new(self.as_line_string::<2>().affine_transform(transform)),
            Polygon(_, XY) => Arc::new(self.as_polygon::<2>().affine_transform(transform)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point::<2>().affine_transform(transform)),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string::<2>().affine_transform(transform))
            }
            MultiPolygon(_, XY) => {
                Arc::new(self.as_multi_polygon::<2>().affine_transform(transform))
            }
            Mixed(_, XY) => Arc::new(self.as_mixed::<2>().affine_transform(transform)),
            GeometryCollection(_, XY) => Arc::new(
                self.as_geometry_collection::<2>()
                    .affine_transform(transform),
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
