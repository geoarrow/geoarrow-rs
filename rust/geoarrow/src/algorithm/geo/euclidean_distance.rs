use crate::array::*;
use crate::scalar::*;
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use arrow_array::builder::Float64Builder;
use arrow_array::Float64Array;
use geo::{Distance, Euclidean};

pub trait EuclideanDistance<Rhs> {
    /// Returns the distance between two geometries
    ///
    /// If a `Point` is contained by a `Polygon`, the distance is `0.0`
    ///
    /// If a `Point` lies on a `Polygon`'s exterior or interior rings, the distance is `0.0`
    ///
    /// If a `Point` lies on a `LineString`, the distance is `0.0`
    ///
    /// The distance between a `Point` and an empty `LineString` is `0.0`
    ///
    /// # Examples
    ///
    /// `Point` to `Point`:
    ///
    /// ```
    /// use approx::assert_relative_eq;
    /// use geo::EuclideanDistance;
    /// use geo::point;
    ///
    /// let p1 = point!(x: -72.1235, y: 42.3521);
    /// let p2 = point!(x: -72.1260, y: 42.45);
    ///
    /// let distance = p1.euclidean_distance(&p2);
    ///
    /// assert_relative_eq!(distance, 0.09793191512474639);
    /// ```
    ///
    /// `Point` to `Polygon`:
    ///
    /// ```
    /// use approx::assert_relative_eq;
    /// use geo::EuclideanDistance;
    /// use geo::{point, polygon};
    ///
    /// let polygon = polygon![
    ///     (x: 5., y: 1.),
    ///     (x: 4., y: 2.),
    ///     (x: 4., y: 3.),
    ///     (x: 5., y: 4.),
    ///     (x: 6., y: 4.),
    ///     (x: 7., y: 3.),
    ///     (x: 7., y: 2.),
    ///     (x: 6., y: 1.),
    ///     (x: 5., y: 1.),
    /// ];
    ///
    /// let point = point!(x: 2.5, y: 0.5);
    ///
    /// let distance = point.euclidean_distance(&polygon);
    ///
    /// assert_relative_eq!(distance, 2.1213203435596424);
    /// ```
    ///
    /// `Point` to `LineString`:
    ///
    /// ```
    /// use approx::assert_relative_eq;
    /// use geo::EuclideanDistance;
    /// use geo::{point, line_string};
    ///
    /// let line_string = line_string![
    ///     (x: 5., y: 1.),
    ///     (x: 4., y: 2.),
    ///     (x: 4., y: 3.),
    ///     (x: 5., y: 4.),
    ///     (x: 6., y: 4.),
    ///     (x: 7., y: 3.),
    ///     (x: 7., y: 2.),
    ///     (x: 6., y: 1.),
    /// ];
    ///
    /// let point = point!(x: 5.5, y: 2.1);
    ///
    /// let distance = point.euclidean_distance(&line_string);
    ///
    /// assert_relative_eq!(distance, 1.1313708498984762);
    /// ```
    fn euclidean_distance(&self, rhs: &Rhs) -> Float64Array;
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl EuclideanDistance<PointArray> for PointArray {
    /// Minimum distance between two Points
    fn euclidean_distance(&self, other: &PointArray) -> Float64Array {
        assert_eq!(self.len(), other.len());
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo()
            .zip(other.iter_geo())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(second)) => {
                    output_array.append_value(Euclidean::distance(&first, &second))
                }
                _ => output_array.append_null(),
            });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a> EuclideanDistance<$second> for $first {
            fn euclidean_distance(&self, other: &$second) -> Float64Array {
                assert_eq!(self.len(), other.len());
                let mut output_array = Float64Builder::with_capacity(self.len());

                self.iter_geo()
                    .zip(other.iter_geo())
                    .for_each(|(first, second)| match (first, second) {
                        (Some(first), Some(second)) => {
                            output_array.append_value(Euclidean::distance(&first, &second))
                        }
                        _ => output_array.append_null(),
                    });

                output_array.finish()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(PointArray, LineStringArray);
iter_geo_impl!(PointArray, PolygonArray);
iter_geo_impl!(PointArray, MultiPointArray);
iter_geo_impl!(PointArray, MultiLineStringArray);
iter_geo_impl!(PointArray, MultiPolygonArray);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray, PointArray);
iter_geo_impl!(LineStringArray, LineStringArray);
iter_geo_impl!(LineStringArray, PolygonArray);
// iter_geo_impl!(LineStringArray, MultiPointArray);
// iter_geo_impl!(LineStringArray, MultiLineStringArray);
// iter_geo_impl!(LineStringArray, MultiPolygonArray);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray, PointArray);
iter_geo_impl!(PolygonArray, LineStringArray);
iter_geo_impl!(PolygonArray, PolygonArray);
// iter_geo_impl!(PolygonArray, MultiPointArray);
// iter_geo_impl!(PolygonArray, MultiLineStringArray);
// iter_geo_impl!(PolygonArray, MultiPolygonArray);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray, PointArray);
// iter_geo_impl!(MultiPointArray, LineStringArray);
// iter_geo_impl!(MultiPointArray, PolygonArray);
// iter_geo_impl!(MultiPointArray, MultiPointArray);
// iter_geo_impl!(MultiPointArray, MultiLineStringArray);
// iter_geo_impl!(MultiPointArray, MultiPolygonArray);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray, PointArray);
// iter_geo_impl!(MultiLineStringArray, LineStringArray);
// iter_geo_impl!(MultiLineStringArray, PolygonArray);
// iter_geo_impl!(MultiLineStringArray, MultiPointArray);
// iter_geo_impl!(MultiLineStringArray, MultiLineStringArray);
// iter_geo_impl!(MultiLineStringArray, MultiPolygonArray);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray, PointArray);
// iter_geo_impl!(MultiPolygonArray, LineStringArray);
// iter_geo_impl!(MultiPolygonArray, PolygonArray);
// iter_geo_impl!(MultiPolygonArray, MultiPointArray);
// iter_geo_impl!(MultiPolygonArray, MultiLineStringArray);
// iter_geo_impl!(MultiPolygonArray, MultiPolygonArray);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl<'a> EuclideanDistance<Point> for PointArray {
    /// Minimum distance between two Points
    fn euclidean_distance(&self, other: &Point) -> Float64Array {
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_point| {
            let output = maybe_point.map(|point| Euclidean::distance(&point, &other.to_geo()));
            output_array.append_option(output)
        });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl_scalar {
    ($first:ty, $second:ty) => {
        impl<'a> EuclideanDistance<$second> for $first {
            fn euclidean_distance(&self, other: &$second) -> Float64Array {
                let mut output_array = Float64Builder::with_capacity(self.len());
                let other_geo = other.to_geo();

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| Euclidean::distance(&geom, &other_geo));
                    output_array.append_option(output)
                });

                output_array.finish()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl_scalar!(PointArray, LineString);
iter_geo_impl_scalar!(PointArray, Polygon);
iter_geo_impl_scalar!(PointArray, MultiPoint);
iter_geo_impl_scalar!(PointArray, MultiLineString);
iter_geo_impl_scalar!(PointArray, MultiPolygon);

// Implementations on LineStringArray
iter_geo_impl_scalar!(LineStringArray, Point);
iter_geo_impl_scalar!(LineStringArray, LineString);
iter_geo_impl_scalar!(LineStringArray, Polygon);
// iter_geo_impl_scalar!(LineStringArray, MultiPoint);
// iter_geo_impl_scalar!(LineStringArray, MultiLineString);
// iter_geo_impl_scalar!(LineStringArray, MultiPolygon);

// Implementations on PolygonArray
iter_geo_impl_scalar!(PolygonArray, Point);
iter_geo_impl_scalar!(PolygonArray, LineString);
iter_geo_impl_scalar!(PolygonArray, Polygon);
// iter_geo_impl_scalar!(PolygonArray, MultiPoint);
// iter_geo_impl_scalar!(PolygonArray, MultiLineString);
// iter_geo_impl_scalar!(PolygonArray, MultiPolygon);

// Implementations on MultiPointArray
iter_geo_impl_scalar!(MultiPointArray, Point);
// iter_geo_impl_scalar!(MultiPointArray, LineString);
// iter_geo_impl_scalar!(MultiPointArray, Polygon);
// iter_geo_impl_scalar!(MultiPointArray, MultiPoint);
// iter_geo_impl_scalar!(MultiPointArray, MultiLineString);
// iter_geo_impl_scalar!(MultiPointArray, MultiPolygon);

// Implementations on MultiLineStringArray
iter_geo_impl_scalar!(MultiLineStringArray, Point);
// iter_geo_impl_scalar!(MultiLineStringArray, LineString);
// iter_geo_impl_scalar!(MultiLineStringArray, Polygon);
// iter_geo_impl_scalar!(MultiLineStringArray, MultiPoint);
// iter_geo_impl_scalar!(MultiLineStringArray, MultiLineString);
// iter_geo_impl_scalar!(MultiLineStringArray, MultiPolygon);

// Implementations on MultiPolygonArray
iter_geo_impl_scalar!(MultiPolygonArray, Point);
// iter_geo_impl_scalar!(MultiPolygonArray, LineString);
// iter_geo_impl_scalar!(MultiPolygonArray, Polygon);
// iter_geo_impl_scalar!(MultiPolygonArray, MultiPoint);
// iter_geo_impl_scalar!(MultiPolygonArray, MultiLineString);
// iter_geo_impl_scalar!(MultiPolygonArray, MultiPolygon);
