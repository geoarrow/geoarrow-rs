use crate::algorithm::broadcasting::{BroadcastablePrimitive, BroadcastableVec};
use crate::algorithm::geo::{AffineOps, Center, Centroid};
use crate::array::MultiPointArray;
use crate::array::*;
use geo::AffineTransform;

/// Rotate a geometry around a point by an angle, in degrees.
///
/// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
///
/// ## Performance
///
/// If you will be performing multiple transformations, like [`Scale`](crate::Scale),
/// [`Skew`](crate::Skew), [`Translate`](crate::Translate), or [`Rotate`](crate::Rotate), it is more
/// efficient to compose the transformations and apply them as a single operation using the
/// [`AffineOps`](crate::AffineOps) trait.
pub trait Rotate {
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
    fn rotate_around_centroid(&self, degrees: BroadcastablePrimitive<f64>) -> Self;

    // /// Mutable version of [`Self::rotate_around_centroid`]
    // fn rotate_around_centroid_mut(&mut self, degrees: f64);

    /// Rotate a geometry around the center of its [bounding box](BoundingRect) by an angle, in
    /// degrees.
    ///
    /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
    ///
    #[must_use]
    fn rotate_around_center(&self, degrees: BroadcastablePrimitive<f64>) -> Self;

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
    fn rotate_around_point(&self, degrees: BroadcastablePrimitive<f64>, point: geo::Point) -> Self;

    // /// Mutable version of [`Self::rotate_around_point`]
    // fn rotate_around_point_mut(&mut self, degrees: f64, point: Point<f64>);
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($type:ident) => {
        impl Rotate for $type {
            fn rotate_around_centroid(&self, degrees: BroadcastablePrimitive<f64>) -> $type {
                let centroids = self.centroid();
                let transforms: Vec<AffineTransform> = centroids
                    .values_iter()
                    .zip(degrees.into_iter())
                    .map(|(point, angle)| {
                        let point: geo::Point = point.into();
                        AffineTransform::rotate(angle, point)
                    })
                    .collect();
                self.affine_transform(BroadcastableVec::Array(transforms))
            }

            fn rotate_around_center(&self, degrees: BroadcastablePrimitive<f64>) -> Self {
                let centers = self.center();
                let transforms: Vec<AffineTransform> = centers
                    .values_iter()
                    .zip(degrees.into_iter())
                    .map(|(point, angle)| {
                        let point: geo::Point = point.into();
                        AffineTransform::rotate(angle, point)
                    })
                    .collect();
                self.affine_transform(BroadcastableVec::Array(transforms))
            }

            fn rotate_around_point(
                &self,
                degrees: BroadcastablePrimitive<f64>,
                point: geo::Point,
            ) -> Self {
                // Note: We need to unpack the enum here because otherwise the scalar will iter forever
                let transforms = match degrees {
                    BroadcastablePrimitive::Scalar(degrees) => {
                        BroadcastableVec::Scalar(AffineTransform::rotate(degrees, point))
                    }
                    BroadcastablePrimitive::Array(degrees) => {
                        let transforms: Vec<AffineTransform> = degrees
                            .values_iter()
                            .map(|degrees| AffineTransform::rotate(*degrees, point))
                            .collect();
                        BroadcastableVec::Array(transforms)
                    }
                };

                self.affine_transform(transforms)
            }
        }
    };
}

iter_geo_impl!(PointArray);
iter_geo_impl!(LineStringArray);
iter_geo_impl!(PolygonArray);
iter_geo_impl!(MultiPointArray);
iter_geo_impl!(MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray);
iter_geo_impl!(WKBArray);

impl Rotate for GeometryArray {
    fn rotate_around_centroid(&self, degrees: BroadcastablePrimitive<f64>) -> Self {
        match self {
            GeometryArray::WKB(arr) => GeometryArray::WKB(arr.rotate_around_centroid(degrees)),
            GeometryArray::Point(arr) => GeometryArray::Point(arr.rotate_around_centroid(degrees)),
            GeometryArray::LineString(arr) => {
                GeometryArray::LineString(arr.rotate_around_centroid(degrees))
            }
            GeometryArray::Polygon(arr) => {
                GeometryArray::Polygon(arr.rotate_around_centroid(degrees))
            }
            GeometryArray::MultiPoint(arr) => {
                GeometryArray::MultiPoint(arr.rotate_around_centroid(degrees))
            }
            GeometryArray::MultiLineString(arr) => {
                GeometryArray::MultiLineString(arr.rotate_around_centroid(degrees))
            }
            GeometryArray::MultiPolygon(arr) => {
                GeometryArray::MultiPolygon(arr.rotate_around_centroid(degrees))
            }
        }
    }

    fn rotate_around_center(&self, degrees: BroadcastablePrimitive<f64>) -> Self {
        match self {
            GeometryArray::WKB(arr) => GeometryArray::WKB(arr.rotate_around_center(degrees)),
            GeometryArray::Point(arr) => GeometryArray::Point(arr.rotate_around_center(degrees)),
            GeometryArray::LineString(arr) => {
                GeometryArray::LineString(arr.rotate_around_center(degrees))
            }
            GeometryArray::Polygon(arr) => {
                GeometryArray::Polygon(arr.rotate_around_center(degrees))
            }
            GeometryArray::MultiPoint(arr) => {
                GeometryArray::MultiPoint(arr.rotate_around_center(degrees))
            }
            GeometryArray::MultiLineString(arr) => {
                GeometryArray::MultiLineString(arr.rotate_around_center(degrees))
            }
            GeometryArray::MultiPolygon(arr) => {
                GeometryArray::MultiPolygon(arr.rotate_around_center(degrees))
            }
        }
    }

    fn rotate_around_point(&self, degrees: BroadcastablePrimitive<f64>, point: geo::Point) -> Self {
        match self {
            GeometryArray::WKB(arr) => GeometryArray::WKB(arr.rotate_around_point(degrees, point)),
            GeometryArray::Point(arr) => {
                GeometryArray::Point(arr.rotate_around_point(degrees, point))
            }
            GeometryArray::LineString(arr) => {
                GeometryArray::LineString(arr.rotate_around_point(degrees, point))
            }
            GeometryArray::Polygon(arr) => {
                GeometryArray::Polygon(arr.rotate_around_point(degrees, point))
            }
            GeometryArray::MultiPoint(arr) => {
                GeometryArray::MultiPoint(arr.rotate_around_point(degrees, point))
            }
            GeometryArray::MultiLineString(arr) => {
                GeometryArray::MultiLineString(arr.rotate_around_point(degrees, point))
            }
            GeometryArray::MultiPolygon(arr) => {
                GeometryArray::MultiPolygon(arr.rotate_around_point(degrees, point))
            }
        }
    }
}
