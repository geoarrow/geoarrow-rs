use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use geo::AffineOps as _AffineOps;
use geo::AffineTransform;

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
pub trait AffineOps<Rhs> {
    type Output;

    /// Apply `transform` immutably, outputting a new geometry.
    #[must_use]
    fn affine_transform(&self, transform: Rhs) -> Self::Output;
}

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

impl AffineOps<&AffineTransform> for PointArray {
    type Output = Self;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        let output_geoms: Vec<Option<geo::Point>> = self
            .iter_geo()
            .map(|maybe_g| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        PointBuilder::from_nullable_points(
            output_geoms.iter().map(|x| x.as_ref()),
            Dimension::XY,
            self.coord_type(),
            self.metadata().clone(),
        )
        .finish()
    }
}

impl AffineOps<&AffineTransform> for RectArray {
    type Output = Self;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        let output_geoms: Vec<Option<geo::Rect>> = self
            .iter_geo()
            .map(|maybe_g| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        RectBuilder::from_nullable_rects(
            output_geoms.iter().map(|x| x.as_ref()),
            Dimension::XY,
            self.metadata().clone(),
        )
        .finish()
    }
}

impl AffineOps<&AffineTransform> for GeometryArray {
    type Output = Result<Self>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        let output_geoms: Vec<Option<geo::Geometry>> = self
            .iter_geo()
            .map(|maybe_g| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        Ok(GeometryBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            self.coord_type(),
            self.metadata().clone(),
            false,
        )?
        .finish())
    }
}

impl AffineOps<&AffineTransform> for GeometryCollectionArray {
    type Output = Result<Self>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        let output_geoms: Vec<Option<geo::GeometryCollection>> = self
            .iter_geo()
            .map(|maybe_g| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        Ok(GeometryCollectionBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            Dimension::XY,
            self.coord_type(),
            self.metadata().clone(),
            false,
        )?
        .finish())
    }
}

impl AffineOps<&AffineTransform> for MixedGeometryArray {
    type Output = Result<Self>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        let output_geoms: Vec<Option<geo::Geometry>> = self
            .iter_geo()
            .map(|maybe_g| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        Ok(MixedGeometryBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            Dimension::XY,
            self.coord_type(),
            self.metadata().clone(),
            false,
        )?
        .finish())
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $method:ident, $geo_type:ty) => {
        impl AffineOps<&AffineTransform> for $type {
            type Output = Self;

            fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .map(|maybe_g| {
                        maybe_g.map(|mut geom| {
                            geom.affine_transform_mut(transform);
                            geom
                        })
                    })
                    .collect();

                <$builder_type>::$method(
                    output_geoms.as_slice(),
                    Dimension::XY,
                    self.coord_type(),
                    self.metadata().clone(),
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
    MultiPointArray,
    MultiPointBuilder,
    from_nullable_multi_points,
    geo::MultiPoint
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

// iter_geo_impl!(LineStringArray, LineStringBuilder, push_line_string);
// iter_geo_impl!(PolygonArray, PolygonBuilder, push_polygon);
// iter_geo_impl!(MultiPointArray, MultiPointBuilder, push_multi_point);
// iter_geo_impl!(
//     MultiLineStringArray,
//     MultiLineStringBuilder,
//     push_multi_line_string
// );
// iter_geo_impl!(MultiPolygonArray, MultiPolygonBuilder, push_multi_polygon);
// iter_geo_impl!(MixedGeometryArray, MixedGeometryBuilder, push_geometry);
// iter_geo_impl!(
//     GeometryCollectionArray,
//     GeometryCollectionBuilder,
//     push_geometry_collection
// );

impl AffineOps<&AffineTransform> for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        macro_rules! impl_downcast {
            ($method:ident) => {
                Arc::new(self.$method().affine_transform(transform))
            };
        }
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, _) => impl_downcast!(as_point),
            LineString(_, _) => impl_downcast!(as_line_string),
            Polygon(_, _) => impl_downcast!(as_polygon),
            MultiPoint(_, _) => impl_downcast!(as_multi_point),
            MultiLineString(_, _) => impl_downcast!(as_multi_line_string),
            MultiPolygon(_, _) => impl_downcast!(as_multi_polygon),
            GeometryCollection(_, _) => {
                Arc::new(self.as_geometry_collection().affine_transform(transform)?)
            }
            Rect(_) => impl_downcast!(as_rect),
            Geometry(_) => Arc::new(self.as_geometry().affine_transform(transform)?),
        };
        Ok(result)
    }
}

impl AffineOps<&AffineTransform> for ChunkedPointArray {
    type Output = Self;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        self.map(|chunk| chunk.affine_transform(transform))
            .try_into()
            .unwrap()
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

impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedRectArray);

macro_rules! impl_try_chunked {
    ($struct_name:ty) => {
        impl AffineOps<&AffineTransform> for $struct_name {
            type Output = Result<Self>;

            fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
                Ok(self
                    .try_map(|chunk| chunk.affine_transform(transform))?
                    .try_into()
                    .unwrap())
            }
        }
    };
}

impl_try_chunked!(ChunkedMixedGeometryArray);
impl_try_chunked!(ChunkedGeometryCollectionArray);

impl AffineOps<&AffineTransform> for &dyn ChunkedNativeArray {
    type Output = Result<Arc<dyn ChunkedNativeArray>>;

    fn affine_transform(&self, transform: &AffineTransform) -> Self::Output {
        macro_rules! impl_downcast {
            ($method:ident) => {
                Arc::new(self.$method().affine_transform(transform))
            };
        }
        use NativeType::*;

        let result: Arc<dyn ChunkedNativeArray> = match self.data_type() {
            Point(_, _) => impl_downcast!(as_point),
            LineString(_, _) => impl_downcast!(as_line_string),
            Polygon(_, _) => impl_downcast!(as_polygon),
            MultiPoint(_, _) => impl_downcast!(as_multi_point),
            MultiLineString(_, _) => impl_downcast!(as_multi_line_string),
            MultiPolygon(_, _) => impl_downcast!(as_multi_polygon),
            GeometryCollection(_, _) => {
                Arc::new(self.as_geometry_collection().affine_transform(transform)?)
            }
            Rect(_) => impl_downcast!(as_rect),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl AffineOps<&[AffineTransform]> for PointArray {
    type Output = Self;

    fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
        let output_geoms: Vec<Option<geo::Point>> = self
            .iter_geo()
            .zip(transform.iter())
            .map(|(maybe_g, transform)| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        PointBuilder::from_nullable_points(
            output_geoms.iter().map(|x| x.as_ref()),
            Dimension::XY,
            self.coord_type(),
            self.metadata().clone(),
        )
        .finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl2 {
    ($type:ty, $builder_type:ty, $method:ident, $geo_type:ty) => {
        impl AffineOps<&[AffineTransform]> for $type {
            type Output = Self;

            fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
                let output_geoms: Vec<Option<$geo_type>> = self
                    .iter_geo()
                    .zip(transform.iter())
                    .map(|(maybe_g, transform)| {
                        maybe_g.map(|mut geom| {
                            geom.affine_transform_mut(transform);
                            geom
                        })
                    })
                    .collect();

                <$builder_type>::$method(
                    output_geoms.as_slice(),
                    Dimension::XY,
                    self.coord_type(),
                    self.metadata().clone(),
                )
                .finish()
            }
        }
    };
}

iter_geo_impl2!(
    LineStringArray,
    LineStringBuilder,
    from_nullable_line_strings,
    geo::LineString
);
iter_geo_impl2!(
    PolygonArray,
    PolygonBuilder,
    from_nullable_polygons,
    geo::Polygon
);
iter_geo_impl2!(
    MultiPointArray,
    MultiPointBuilder,
    from_nullable_multi_points,
    geo::MultiPoint
);
iter_geo_impl2!(
    MultiLineStringArray,
    MultiLineStringBuilder,
    from_nullable_multi_line_strings,
    geo::MultiLineString
);
iter_geo_impl2!(
    MultiPolygonArray,
    MultiPolygonBuilder,
    from_nullable_multi_polygons,
    geo::MultiPolygon
);

impl AffineOps<&[AffineTransform]> for GeometryCollectionArray {
    type Output = Result<Self>;

    fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
        let output_geoms: Vec<Option<geo::GeometryCollection>> = self
            .iter_geo()
            .zip(transform.iter())
            .map(|(maybe_g, transform)| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        Ok(GeometryCollectionBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            Dimension::XY,
            self.coord_type(),
            self.metadata().clone(),
            false,
        )?
        .finish())
    }
}

impl AffineOps<&[AffineTransform]> for MixedGeometryArray {
    type Output = Result<Self>;

    fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
        let output_geoms: Vec<Option<geo::Geometry>> = self
            .iter_geo()
            .zip(transform.iter())
            .map(|(maybe_g, transform)| {
                maybe_g.map(|mut geom| {
                    geom.affine_transform_mut(transform);
                    geom
                })
            })
            .collect();

        Ok(MixedGeometryBuilder::from_nullable_geometries(
            output_geoms.as_slice(),
            Dimension::XY,
            self.coord_type(),
            self.metadata().clone(),
            false,
        )?
        .finish())
    }
}

impl AffineOps<&[AffineTransform]> for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn affine_transform(&self, transform: &[AffineTransform]) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => Arc::new(self.as_point().affine_transform(transform)),
            LineString(_, XY) => Arc::new(self.as_line_string().affine_transform(transform)),
            Polygon(_, XY) => Arc::new(self.as_polygon().affine_transform(transform)),
            MultiPoint(_, XY) => Arc::new(self.as_multi_point().affine_transform(transform)),
            MultiLineString(_, XY) => {
                Arc::new(self.as_multi_line_string().affine_transform(transform))
            }
            MultiPolygon(_, XY) => Arc::new(self.as_multi_polygon().affine_transform(transform)),
            GeometryCollection(_, XY) => {
                Arc::new(self.as_geometry_collection().affine_transform(transform)?)
            }
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}
