use std::sync::Arc;

use crate::algorithm::geo::{AffineOps, Center, Centroid};
use crate::array::MultiPointArray;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::Result;
use crate::trait_::ArrayAccessor;
use crate::NativeArray;
use arrow_array::Float64Array;
use geo::AffineTransform;

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
pub trait Rotate<DegreesT> {
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
    fn rotate_around_centroid(&self, degrees: &DegreesT) -> Self::Output;

    /// Rotate a geometry around the center of its [bounding
    /// box](crate::algorithm::geo::BoundingRect) by an angle, in degrees.
    ///
    /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
    ///
    #[must_use]
    fn rotate_around_center(&self, degrees: &DegreesT) -> Self::Output;

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
    fn rotate_around_point(&self, degrees: &DegreesT, point: geo::Point) -> Self::Output;
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Rotate<Float64Array> for PointArray<2> {
    type Output = Self;

    fn rotate_around_centroid(&self, degrees: &Float64Array) -> Self {
        let centroids = self.centroid();
        let transforms: Vec<AffineTransform> = centroids
            .iter_geo_values()
            .zip(degrees.values().iter())
            .map(|(point, angle)| AffineTransform::rotate(*angle, point))
            .collect();
        self.affine_transform(transforms.as_slice())
    }

    fn rotate_around_center(&self, degrees: &Float64Array) -> Self {
        let centers = self.center();
        let transforms: Vec<AffineTransform> = centers
            .iter_geo_values()
            .zip(degrees.values().iter())
            .map(|(point, angle)| AffineTransform::rotate(*angle, point))
            .collect();
        self.affine_transform(transforms.as_slice())
    }

    fn rotate_around_point(&self, degrees: &Float64Array, point: geo::Point) -> Self {
        let transforms: Vec<AffineTransform> = degrees
            .values()
            .iter()
            .map(|degrees| AffineTransform::rotate(*degrees, point))
            .collect();
        self.affine_transform(transforms.as_slice())
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl Rotate<Float64Array> for $type {
            type Output = Self;

            fn rotate_around_centroid(&self, degrees: &Float64Array) -> $type {
                let centroids = self.centroid();
                let transforms: Vec<AffineTransform> = centroids
                    .iter_geo_values()
                    .zip(degrees.values().iter())
                    .map(|(point, angle)| AffineTransform::rotate(*angle, point))
                    .collect();
                self.affine_transform(transforms.as_slice())
            }

            fn rotate_around_center(&self, degrees: &Float64Array) -> Self {
                let centers = self.center();
                let transforms: Vec<AffineTransform> = centers
                    .iter_geo_values()
                    .zip(degrees.values().iter())
                    .map(|(point, angle)| AffineTransform::rotate(*angle, point))
                    .collect();
                self.affine_transform(transforms.as_slice())
            }

            fn rotate_around_point(&self, degrees: &Float64Array, point: geo::Point) -> Self {
                let transforms: Vec<AffineTransform> = degrees
                    .values()
                    .iter()
                    .map(|degrees| AffineTransform::rotate(*degrees, point))
                    .collect();
                self.affine_transform(transforms.as_slice())
            }
        }
    };
}

iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Rotate<f64> for PointArray<2> {
    type Output = Self;

    fn rotate_around_centroid(&self, degrees: &f64) -> Self {
        let centroids = self.centroid();
        let transforms: Vec<AffineTransform> = centroids
            .iter_geo_values()
            .map(|point| AffineTransform::rotate(*degrees, point))
            .collect();
        self.affine_transform(transforms.as_slice())
    }

    fn rotate_around_center(&self, degrees: &f64) -> Self {
        let centers = self.center();
        let transforms: Vec<AffineTransform> = centers
            .iter_geo_values()
            .map(|point| AffineTransform::rotate(*degrees, point))
            .collect();
        self.affine_transform(transforms.as_slice())
    }

    fn rotate_around_point(&self, degrees: &f64, point: geo::Point) -> Self {
        let transform = AffineTransform::rotate(*degrees, point);
        self.affine_transform(&transform)
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl_scalar {
    ($type:ty) => {
        impl Rotate<f64> for $type {
            type Output = Self;

            fn rotate_around_centroid(&self, degrees: &f64) -> $type {
                let centroids = self.centroid();
                let transforms: Vec<AffineTransform> = centroids
                    .iter_geo_values()
                    .map(|point| AffineTransform::rotate(*degrees, point))
                    .collect();
                self.affine_transform(transforms.as_slice())
            }

            fn rotate_around_center(&self, degrees: &f64) -> Self {
                let centers = self.center();
                let transforms: Vec<AffineTransform> = centers
                    .iter_geo_values()
                    .map(|point| AffineTransform::rotate(*degrees, point))
                    .collect();
                self.affine_transform(transforms.as_slice())
            }

            fn rotate_around_point(&self, degrees: &f64, point: geo::Point) -> Self {
                let transform = AffineTransform::rotate(*degrees, point);
                self.affine_transform(&transform)
            }
        }
    };
}

iter_geo_impl_scalar!(LineStringArray<2>);
iter_geo_impl_scalar!(PolygonArray<2>);
iter_geo_impl_scalar!(MultiPointArray<2>);
iter_geo_impl_scalar!(MultiLineStringArray<2>);
iter_geo_impl_scalar!(MultiPolygonArray<2>);

impl Rotate<f64> for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn rotate_around_centroid(&self, degrees: &f64) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_centroid(degrees))
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // WKB => impl_method!(as_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }

    fn rotate_around_center(&self, degrees: &f64) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_center(degrees))
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // WKB => impl_method!(as_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }

    fn rotate_around_point(&self, degrees: &f64, point: geo::Point) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_point(degrees, point))
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // WKB => impl_method!(as_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }
}

impl Rotate<Float64Array> for &dyn NativeArray {
    type Output = Result<Arc<dyn NativeArray>>;

    fn rotate_around_centroid(&self, degrees: &Float64Array) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_centroid(degrees))
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // WKB => impl_method!(as_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }

    fn rotate_around_center(&self, degrees: &Float64Array) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_center(degrees))
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // WKB => impl_method!(as_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }

    fn rotate_around_point(&self, degrees: &Float64Array, point: geo::Point) -> Self::Output {
        macro_rules! impl_method {
            ($method:ident) => {{
                Arc::new(self.$method().rotate_around_point(degrees, point))
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: Arc<dyn NativeArray> = match self.data_type() {
            Point(_, XY) => impl_method!(as_point),
            LineString(_, XY) => impl_method!(as_line_string),
            Polygon(_, XY) => impl_method!(as_polygon),
            MultiPoint(_, XY) => impl_method!(as_multi_point),
            MultiLineString(_, XY) => impl_method!(as_multi_line_string),
            MultiPolygon(_, XY) => impl_method!(as_multi_polygon),
            // Mixed(_, XY) => impl_method!(as_mixed),
            // GeometryCollection(_, XY) => impl_method!(as_geometry_collection),
            // WKB => impl_method!(as_wkb),
            // Rect(XY) => impl_method!(as_rect),
            _ => todo!("unsupported data type"),
        };

        Ok(result)
    }
}
