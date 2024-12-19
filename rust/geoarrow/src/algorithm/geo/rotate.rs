use std::sync::Arc;

use crate::algorithm::broadcasting::{BroadcastablePoint, BroadcastablePrimitive};
use crate::array::MultiPointArray;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow::datatypes::Float64Type;
use geo::Rotate as _;

/// Rotate geometries around a point by an angle, in degrees.
///
/// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
///
/// ## Performance
///
/// If you will be performing multiple transformations, like
/// [`Scale`](crate::algorithm::geo::Scale), [`Skew`](crate::algorithm::geo::Skew),
/// [`Translate`](crate::algorithm::geo::Translate), or [`Rotate`](crate::algorithm::geo::Rotate),
/// it is more efficient to compose the transformations and apply them as a single operation using
/// the [`AffineOps`](crate::algorithm::geo::AffineOps) trait.
pub trait Rotate {
    type Output;

    /// Rotate a geometry around its [centroid](Centroid) by an angle, in degrees
    ///
    /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Rotate;
    /// use geo::line_string;
    /// use approx::assert_relative_eq;
    ///
    /// let line_string = line_string![
    ///     (x: 0.0, y: 0.0),
    ///     (x: 5.0, y: 5.0),
    ///     (x: 10.0, y: 10.0),
    /// ];
    ///
    /// let rotated = line_string.rotate_around_centroid(-45.0);
    ///
    /// let expected = line_string![
    ///     (x: -2.071067811865475, y: 5.0),
    ///     (x: 5.0, y: 5.0),
    ///     (x: 12.071067811865476, y: 5.0),
    /// ];
    ///
    /// assert_relative_eq!(expected, rotated);
    /// ```
    #[must_use]
    fn rotate_around_centroid(&self, degrees: &BroadcastablePrimitive<Float64Type>)
        -> Self::Output;

    /// Rotate a geometry around the center of its [bounding
    /// box](crate::algorithm::geo::BoundingRect) by an angle, in degrees.
    ///
    /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
    ///
    #[must_use]
    fn rotate_around_center(&self, degrees: &BroadcastablePrimitive<Float64Type>) -> Self::Output;

    /// Rotate a Geometry around an arbitrary point by an angle, given in degrees
    ///
    /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
    ///
    /// # Examples
    ///
    /// ```
    /// use geo::Rotate;
    /// use geo::{line_string, point};
    ///
    /// let ls = line_string![
    ///     (x: 0.0, y: 0.0),
    ///     (x: 5.0, y: 5.0),
    ///     (x: 10.0, y: 10.0)
    /// ];
    ///
    /// let rotated = ls.rotate_around_point(
    ///     -45.0,
    ///     point!(x: 10.0, y: 0.0),
    /// );
    ///
    /// assert_eq!(rotated, line_string![
    ///     (x: 2.9289321881345245, y: 7.071067811865475),
    ///     (x: 10.0, y: 7.0710678118654755),
    ///     (x: 17.071067811865476, y: 7.0710678118654755)
    /// ]);
    /// ```
    #[must_use]
    fn rotate_around_point(
        &self,
        degrees: &BroadcastablePrimitive<Float64Type>,
        point: &BroadcastablePoint,
    ) -> Self::Output;
}

impl Rotate for PointArray {
    type Output = Self;

    fn rotate_around_centroid(&self, degrees: &BroadcastablePrimitive<Float64Type>) -> PointArray {
        let mut builder = PointBuilder::with_capacity_and_options(
            Dimension::XY,
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata().clone(),
        );

        self.iter_geo().zip(degrees).for_each(|(maybe_g, degrees)| {
            if let (Some(mut geom), Some(degrees)) = (maybe_g, degrees) {
                geom.rotate_around_centroid_mut(degrees);
                builder.push_point(Some(&geom));
            } else {
                builder.push_null();
            }
        });

        builder.finish()
    }

    fn rotate_around_center(&self, degrees: &BroadcastablePrimitive<Float64Type>) -> Self {
        let mut builder = PointBuilder::with_capacity_and_options(
            Dimension::XY,
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata().clone(),
        );

        self.iter_geo().zip(degrees).for_each(|(maybe_g, degrees)| {
            if let (Some(mut geom), Some(degrees)) = (maybe_g, degrees) {
                geom.rotate_around_center_mut(degrees);
                builder.push_point(Some(&geom));
            } else {
                builder.push_null();
            }
        });

        builder.finish()
    }

    fn rotate_around_point(
        &self,
        degrees: &BroadcastablePrimitive<Float64Type>,
        point: &BroadcastablePoint,
    ) -> Self {
        let mut builder = PointBuilder::with_capacity_and_options(
            Dimension::XY,
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata().clone(),
        );

        self.iter_geo()
            .zip(degrees)
            .zip(point)
            .for_each(|((maybe_g, degrees), point)| {
                if let (Some(mut geom), Some(degrees), Some(point)) = (maybe_g, degrees, point) {
                    geom.rotate_around_point_mut(degrees, point);
                    builder.push_point(Some(&geom));
                } else {
                    builder.push_null();
                }
            });

        builder.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty, $builder_type:ty, $push_func:ident) => {
        impl Rotate for $type {
            type Output = Self;

            fn rotate_around_centroid(
                &self,
                degrees: &BroadcastablePrimitive<Float64Type>,
            ) -> Self {
                let mut builder = <$builder_type>::with_capacity_and_options(
                    Dimension::XY,
                    self.buffer_lengths(),
                    self.coord_type(),
                    self.metadata().clone(),
                );

                self.iter_geo().zip(degrees).for_each(|(maybe_g, degrees)| {
                    if let (Some(mut geom), Some(degrees)) = (maybe_g, degrees) {
                        geom.rotate_around_centroid_mut(degrees);
                        builder.$push_func(Some(&geom)).unwrap();
                    } else {
                        builder.push_null();
                    }
                });

                builder.finish()
            }

            fn rotate_around_center(&self, degrees: &BroadcastablePrimitive<Float64Type>) -> Self {
                let mut builder = <$builder_type>::with_capacity_and_options(
                    Dimension::XY,
                    self.buffer_lengths(),
                    self.coord_type(),
                    self.metadata().clone(),
                );

                self.iter_geo().zip(degrees).for_each(|(maybe_g, degrees)| {
                    if let (Some(mut geom), Some(degrees)) = (maybe_g, degrees) {
                        geom.rotate_around_center_mut(degrees);
                        builder.$push_func(Some(&geom)).unwrap();
                    } else {
                        builder.push_null();
                    }
                });

                builder.finish()
            }

            fn rotate_around_point(
                &self,
                degrees: &BroadcastablePrimitive<Float64Type>,
                point: &BroadcastablePoint,
            ) -> Self {
                let mut builder = <$builder_type>::with_capacity_and_options(
                    Dimension::XY,
                    self.buffer_lengths(),
                    self.coord_type(),
                    self.metadata().clone(),
                );

                self.iter_geo()
                    .zip(degrees)
                    .zip(point)
                    .for_each(|((maybe_g, degrees), point)| {
                        if let (Some(mut geom), Some(degrees), Some(point)) =
                            (maybe_g, degrees, point)
                        {
                            geom.rotate_around_point_mut(degrees, point);
                            builder.$push_func(Some(&geom)).unwrap();
                        } else {
                            builder.push_null();
                        }
                    });

                builder.finish()
            }
        }
    };
}

iter_geo_impl!(LineStringArray, LineStringBuilder, push_line_string);
iter_geo_impl!(PolygonArray, PolygonBuilder, push_polygon);
iter_geo_impl!(MultiPointArray, MultiPointBuilder, push_multi_point);
iter_geo_impl!(
    MultiLineStringArray,
    MultiLineStringBuilder,
    push_multi_line_string
);
iter_geo_impl!(MultiPolygonArray, MultiPolygonBuilder, push_multi_polygon);
// iter_geo_impl!(GeometryArray, GeometryBuilder, push_geometry);

impl Rotate for GeometryArray {
    type Output = Result<Self>;

    fn rotate_around_centroid(
        &self,
        degrees: &BroadcastablePrimitive<Float64Type>,
    ) -> Self::Output {
        let mut builder = GeometryBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata().clone(),
            false,
        );

        self.iter_geo()
            .zip(degrees)
            .try_for_each(|(maybe_g, degrees)| {
                if let (Some(mut geom), Some(degrees)) = (maybe_g, degrees) {
                    geom.rotate_around_centroid_mut(degrees);
                    builder.push_geometry(Some(&geom))?;
                } else {
                    builder.push_null();
                }
                Ok::<_, GeoArrowError>(())
            })?;

        Ok(builder.finish())
    }

    fn rotate_around_center(&self, degrees: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        let mut builder = GeometryBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata().clone(),
            false,
        );

        self.iter_geo()
            .zip(degrees)
            .try_for_each(|(maybe_g, degrees)| {
                if let (Some(mut geom), Some(degrees)) = (maybe_g, degrees) {
                    geom.rotate_around_center_mut(degrees);
                    builder.push_geometry(Some(&geom))?;
                } else {
                    builder.push_null();
                }
                Ok::<_, GeoArrowError>(())
            })?;

        Ok(builder.finish())
    }

    fn rotate_around_point(
        &self,
        degrees: &BroadcastablePrimitive<Float64Type>,
        point: &BroadcastablePoint,
    ) -> Self::Output {
        let mut builder = GeometryBuilder::with_capacity_and_options(
            self.buffer_lengths(),
            self.coord_type(),
            self.metadata().clone(),
            false,
        );

        self.iter_geo()
            .zip(degrees)
            .zip(point)
            .try_for_each(|((maybe_g, degrees), point)| {
                if let (Some(mut geom), Some(degrees), Some(point)) = (maybe_g, degrees, point) {
                    geom.rotate_around_point_mut(degrees, point);
                    builder.push_geometry(Some(&geom))?;
                } else {
                    builder.push_null();
                }
                Ok::<_, GeoArrowError>(())
            })?;

        Ok(builder.finish())
    }
}

impl Rotate for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn rotate_around_centroid(
        &self,
        degrees: &BroadcastablePrimitive<Float64Type>,
    ) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_centroid(degrees))
            }};
        }

        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, _) => impl_method!(as_point),
            LineString(_, _) => impl_method!(as_line_string),
            Polygon(_, _) => impl_method!(as_polygon),
            MultiPoint(_, _) => impl_method!(as_multi_point),
            MultiLineString(_, _) => impl_method!(as_multi_line_string),
            MultiPolygon(_, _) => impl_method!(as_multi_polygon),
            Geometry(_) => Arc::new(self.as_geometry().rotate_around_centroid(degrees)?),
            // GeometryCollection(_, _) => impl_method!(as_geometry_collection),
            // Rect(_) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }

    fn rotate_around_center(&self, degrees: &BroadcastablePrimitive<Float64Type>) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_center(degrees))
            }};
        }

        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, _) => impl_method!(as_point),
            LineString(_, _) => impl_method!(as_line_string),
            Polygon(_, _) => impl_method!(as_polygon),
            MultiPoint(_, _) => impl_method!(as_multi_point),
            MultiLineString(_, _) => impl_method!(as_multi_line_string),
            MultiPolygon(_, _) => impl_method!(as_multi_polygon),
            Geometry(_) => Arc::new(self.as_geometry().rotate_around_centroid(degrees)?),
            // GeometryCollection(_, _) => impl_method!(as_geometry_collection),
            // Rect(_) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }

    fn rotate_around_point(
        &self,
        degrees: &BroadcastablePrimitive<Float64Type>,
        point: &BroadcastablePoint,
    ) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_point(degrees, point))
            }};
        }

        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, _) => impl_method!(as_point),
            LineString(_, _) => impl_method!(as_line_string),
            Polygon(_, _) => impl_method!(as_polygon),
            MultiPoint(_, _) => impl_method!(as_multi_point),
            MultiLineString(_, _) => impl_method!(as_multi_line_string),
            MultiPolygon(_, _) => impl_method!(as_multi_polygon),
            Geometry(_) => Arc::new(self.as_geometry().rotate_around_centroid(degrees)?),
            // GeometryCollection(_, _) => impl_method!(as_geometry_collection),
            // Rect(_) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }
}
