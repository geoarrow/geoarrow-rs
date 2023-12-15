use crate::algorithm::geo::{AffineOps, Center, Centroid};
use crate::array::MultiPointArray;
use crate::array::*;
use arrow_array::{Float64Array, OffsetSizeTrait};
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
    fn rotate_around_centroid(&self, degrees: &DegreesT) -> Self;

    // /// Mutable version of [`Self::rotate_around_centroid`]
    // fn rotate_around_centroid_mut(&mut self, degrees: f64);

    /// Rotate a geometry around the center of its [bounding
    /// box](crate::algorithm::geo::BoundingRect) by an angle, in degrees.
    ///
    /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
    ///
    #[must_use]
    fn rotate_around_center(&self, degrees: &DegreesT) -> Self;

    // /// Mutable version of [`Self::rotate_around_center`]
    // fn rotate_around_center_mut(&mut self, degrees: f64);

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
    fn rotate_around_point(&self, degrees: &DegreesT, point: geo::Point) -> Self;

    // /// Mutable version of [`Self::rotate_around_point`]
    // fn rotate_around_point_mut(&mut self, degrees: f64, point: Point<f64>);
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Rotate<Float64Array> for PointArray {
    fn rotate_around_centroid(&self, degrees: &Float64Array) -> Self {
        let centroids = self.centroid();
        let transforms: Vec<AffineTransform> = centroids
            .iter_geo_values()
            .zip(degrees.values().iter())
            .map(|(point, angle)| AffineTransform::rotate(*angle, point))
            .collect();
        self.affine_transform(&transforms)
    }

    fn rotate_around_center(&self, degrees: &Float64Array) -> Self {
        let centers = self.center();
        let transforms: Vec<AffineTransform> = centers
            .iter_geo_values()
            .zip(degrees.values().iter())
            .map(|(point, angle)| AffineTransform::rotate(*angle, point))
            .collect();
        self.affine_transform(&transforms)
    }

    fn rotate_around_point(&self, degrees: &Float64Array, point: geo::Point) -> Self {
        let transforms: Vec<AffineTransform> = degrees
            .values()
            .iter()
            .map(|degrees| AffineTransform::rotate(*degrees, point))
            .collect();
        self.affine_transform(&transforms)
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Rotate<Float64Array> for $type {
            fn rotate_around_centroid(&self, degrees: &Float64Array) -> $type {
                let centroids = self.centroid();
                let transforms: Vec<AffineTransform> = centroids
                    .iter_geo_values()
                    .zip(degrees.values().iter())
                    .map(|(point, angle)| AffineTransform::rotate(*angle, point))
                    .collect();
                self.affine_transform(&transforms)
            }

            fn rotate_around_center(&self, degrees: &Float64Array) -> Self {
                let centers = self.center();
                let transforms: Vec<AffineTransform> = centers
                    .iter_geo_values()
                    .zip(degrees.values().iter())
                    .map(|(point, angle)| AffineTransform::rotate(*angle, point))
                    .collect();
                self.affine_transform(&transforms)
            }

            fn rotate_around_point(&self, degrees: &Float64Array, point: geo::Point) -> Self {
                let transforms: Vec<AffineTransform> = degrees
                    .values()
                    .iter()
                    .map(|degrees| AffineTransform::rotate(*degrees, point))
                    .collect();
                self.affine_transform(&transforms)
            }
        }
    };
}

iter_geo_impl!(LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>);
iter_geo_impl!(MultiPointArray<O>);
iter_geo_impl!(MultiLineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>);

impl<O: OffsetSizeTrait> Rotate<Float64Array> for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn rotate_around_centroid(&self, degrees: &Float64Array) -> Self;
        fn rotate_around_center(&self, degrees: &Float64Array) -> Self;
        fn rotate_around_point(&self, degrees: &Float64Array, point: geo::Point) -> Self;
    }
}

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

// Note: this can't (easily) be parameterized in the macro because PointArray is not generic over O
impl Rotate<f64> for PointArray {
    fn rotate_around_centroid(&self, degrees: &f64) -> Self {
        let centroids = self.centroid();
        let transforms: Vec<AffineTransform> = centroids
            .iter_geo_values()
            .map(|point| AffineTransform::rotate(*degrees, point))
            .collect();
        self.affine_transform(&transforms)
    }

    fn rotate_around_center(&self, degrees: &f64) -> Self {
        let centers = self.center();
        let transforms: Vec<AffineTransform> = centers
            .iter_geo_values()
            .map(|point| AffineTransform::rotate(*degrees, point))
            .collect();
        self.affine_transform(&transforms)
    }

    fn rotate_around_point(&self, degrees: &f64, point: geo::Point) -> Self {
        let transform = AffineTransform::rotate(*degrees, point);
        self.affine_transform(&transform)
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl_scalar {
    ($type:ty) => {
        impl<O: OffsetSizeTrait> Rotate<f64> for $type {
            fn rotate_around_centroid(&self, degrees: &f64) -> $type {
                let centroids = self.centroid();
                let transforms: Vec<AffineTransform> = centroids
                    .iter_geo_values()
                    .map(|point| AffineTransform::rotate(*degrees, point))
                    .collect();
                self.affine_transform(&transforms)
            }

            fn rotate_around_center(&self, degrees: &f64) -> Self {
                let centers = self.center();
                let transforms: Vec<AffineTransform> = centers
                    .iter_geo_values()
                    .map(|point| AffineTransform::rotate(*degrees, point))
                    .collect();
                self.affine_transform(&transforms)
            }

            fn rotate_around_point(&self, degrees: &f64, point: geo::Point) -> Self {
                let transform = AffineTransform::rotate(*degrees, point);
                self.affine_transform(&transform)
            }
        }
    };
}

iter_geo_impl_scalar!(LineStringArray<O>);
iter_geo_impl_scalar!(PolygonArray<O>);
iter_geo_impl_scalar!(MultiPointArray<O>);
iter_geo_impl_scalar!(MultiLineStringArray<O>);
iter_geo_impl_scalar!(MultiPolygonArray<O>);

impl<O: OffsetSizeTrait> Rotate<f64> for GeometryArray<O> {
    crate::geometry_array_delegate_impl! {
        fn rotate_around_centroid(&self, degrees: &f64) -> Self;
        fn rotate_around_center(&self, degrees: &f64) -> Self;
        fn rotate_around_point(&self, degrees: &f64, point: geo::Point) -> Self;
    }
}
