use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
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

    // TODO: add COW API for affine_transform_mut
    //
    // /// Apply `transform` to mutate `self`.
    // fn affine_transform_mut(&mut self, transform: &AffineTransform<T>);
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
        impl<O: OffsetSizeTrait> AffineOps<&AffineTransform> for $type {
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

iter_geo_impl!(LineStringArray<O, 2>, LineStringBuilder<O, 2>, push_line_string);
iter_geo_impl!(PolygonArray<O, 2>, PolygonBuilder<O, 2>, push_polygon);
iter_geo_impl!(MultiPointArray<O, 2>, MultiPointBuilder<O, 2>, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray<O, 2>,
    MultiLineStringBuilder<O, 2>,
    push_multi_line_string
);
iter_geo_impl!(
    MultiPolygonArray<O, 2>,
    MultiPolygonBuilder<O, 2>,
    push_multi_polygon
);
iter_geo_impl!(
    MixedGeometryArray<O, 2>,
    MixedGeometryBuilder<O, 2>,
    push_geometry
);
iter_geo_impl!(
    GeometryCollectionArray<O, 2>,
    GeometryCollectionBuilder<O, 2>,
    push_geometry_collection
);

impl AffineOps<&AffineTransform> for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        macro_rules! impl_downcast {
            ($method:ident) => {
                Arc::new(self.$method().affine_transform(transform))
            };
        }
        use GeoDataType::*;

        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            Point(_) => impl_downcast!(as_point),
            LineString(_) => impl_downcast!(as_line_string),
            LargeLineString(_) => impl_downcast!(as_large_line_string),
            Polygon(_) => impl_downcast!(as_polygon),
            LargePolygon(_) => impl_downcast!(as_large_polygon),
            MultiPoint(_) => impl_downcast!(as_multi_point),
            LargeMultiPoint(_) => impl_downcast!(as_large_multi_point),
            MultiLineString(_) => impl_downcast!(as_multi_line_string),
            LargeMultiLineString(_) => impl_downcast!(as_large_multi_line_string),
            MultiPolygon(_) => impl_downcast!(as_multi_polygon),
            LargeMultiPolygon(_) => impl_downcast!(as_large_multi_polygon),
            Mixed(_) => impl_downcast!(as_mixed),
            LargeMixed(_) => impl_downcast!(as_large_mixed),
            GeometryCollection(_) => impl_downcast!(as_geometry_collection),
            LargeGeometryCollection(_) => impl_downcast!(as_large_geometry_collection),
            // WKB => impl_downcast!(as_wkb),
            // LargeWKB => impl_downcast!(as_large_wkb),
            // Rect => impl_downcast!(as_rect),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl AffineOps<&AffineTransform> for ChunkedPointArray<2> {
    type Output = Self;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        self.map(|chunk| chunk.affine_transform(transform))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_chunked {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait> AffineOps<&AffineTransform> for $struct_name {
            type Output = Self;

            fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
                self.map(|chunk| chunk.affine_transform(transform))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_chunked!(ChunkedLineStringArray<O, 2>);
impl_chunked!(ChunkedPolygonArray<O, 2>);
impl_chunked!(ChunkedMultiPointArray<O, 2>);
impl_chunked!(ChunkedMultiLineStringArray<O, 2>);
impl_chunked!(ChunkedMultiPolygonArray<O, 2>);
impl_chunked!(ChunkedMixedGeometryArray<O, 2>);
impl_chunked!(ChunkedGeometryCollectionArray<O, 2>);

impl AffineOps<&AffineTransform> for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<Arc<dyn ChunkedGeometryArrayTrait>>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        macro_rules! impl_downcast {
            ($method:ident) => {
                Arc::new(self.$method().affine_transform(transform))
            };
        }
        use GeoDataType::*;

        let result: Arc<dyn ChunkedGeometryArrayTrait> = match self.data_type() {
            Point(_) => impl_downcast!(as_point),
            LineString(_) => impl_downcast!(as_line_string),
            LargeLineString(_) => impl_downcast!(as_large_line_string),
            Polygon(_) => impl_downcast!(as_polygon),
            LargePolygon(_) => impl_downcast!(as_large_polygon),
            MultiPoint(_) => impl_downcast!(as_multi_point),
            LargeMultiPoint(_) => impl_downcast!(as_large_multi_point),
            MultiLineString(_) => impl_downcast!(as_multi_line_string),
            LargeMultiLineString(_) => impl_downcast!(as_large_multi_line_string),
            MultiPolygon(_) => impl_downcast!(as_multi_polygon),
            LargeMultiPolygon(_) => impl_downcast!(as_large_multi_polygon),
            Mixed(_) => impl_downcast!(as_mixed),
            LargeMixed(_) => impl_downcast!(as_large_mixed),
            GeometryCollection(_) => impl_downcast!(as_geometry_collection),
            LargeGeometryCollection(_) => impl_downcast!(as_large_geometry_collection),
            // WKB => impl_downcast!(as_wkb),
            // LargeWKB => impl_downcast!(as_large_wkb),
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
        impl<O: OffsetSizeTrait> AffineOps<&[AffineTransform]> for $type {
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

iter_geo_impl2!(LineStringArray<O, 2>, LineStringBuilder<O, 2>, push_line_string);
iter_geo_impl2!(PolygonArray<O, 2>, PolygonBuilder<O, 2>, push_polygon);
iter_geo_impl2!(MultiPointArray<O, 2>, MultiPointBuilder<O, 2>, push_multi_point);
iter_geo_impl2!(
    MultiLineStringArray<O, 2>,
    MultiLineStringBuilder<O, 2>,
    push_multi_line_string
);
iter_geo_impl2!(
    MultiPolygonArray<O, 2>,
    MultiPolygonBuilder<O, 2>,
    push_multi_polygon
);
iter_geo_impl2!(
    MixedGeometryArray<O, 2>,
    MixedGeometryBuilder<O, 2>,
    push_geometry
);
iter_geo_impl2!(
    GeometryCollectionArray<O, 2>,
    GeometryCollectionBuilder<O, 2>,
    push_geometry_collection
);

impl AffineOps<&[AffineTransform]> for &dyn GeometryArrayTrait {
    type Output = Result<Arc<dyn GeometryArrayTrait>>;

    fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
        let result: Arc<dyn GeometryArrayTrait> = match self.data_type() {
            GeoDataType::Point(_) => Arc::new(self.as_point().affine_transform(transform)),
            GeoDataType::LineString(_) => {
                Arc::new(self.as_line_string().affine_transform(transform))
            }
            GeoDataType::LargeLineString(_) => {
                Arc::new(self.as_large_line_string().affine_transform(transform))
            }
            GeoDataType::Polygon(_) => Arc::new(self.as_polygon().affine_transform(transform)),
            GeoDataType::LargePolygon(_) => {
                Arc::new(self.as_large_polygon().affine_transform(transform))
            }
            GeoDataType::MultiPoint(_) => {
                Arc::new(self.as_multi_point().affine_transform(transform))
            }
            GeoDataType::LargeMultiPoint(_) => {
                Arc::new(self.as_large_multi_point().affine_transform(transform))
            }
            GeoDataType::MultiLineString(_) => {
                Arc::new(self.as_multi_line_string().affine_transform(transform))
            }
            GeoDataType::LargeMultiLineString(_) => Arc::new(
                self.as_large_multi_line_string()
                    .affine_transform(transform),
            ),
            GeoDataType::MultiPolygon(_) => {
                Arc::new(self.as_multi_polygon().affine_transform(transform))
            }
            GeoDataType::LargeMultiPolygon(_) => {
                Arc::new(self.as_large_multi_polygon().affine_transform(transform))
            }
            GeoDataType::Mixed(_) => Arc::new(self.as_mixed().affine_transform(transform)),
            GeoDataType::LargeMixed(_) => {
                Arc::new(self.as_large_mixed().affine_transform(transform))
            }
            GeoDataType::GeometryCollection(_) => {
                Arc::new(self.as_geometry_collection().affine_transform(transform))
            }
            GeoDataType::LargeGeometryCollection(_) => Arc::new(
                self.as_large_geometry_collection()
                    .affine_transform(transform),
            ),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
