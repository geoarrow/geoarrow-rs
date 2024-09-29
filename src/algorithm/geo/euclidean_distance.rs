use crate::array::*;
use crate::scalar::*;
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use arrow_array::builder::Float64Builder;
use arrow_array::{Float64Array, OffsetSizeTrait};
use geo::EuclideanDistance as _EuclideanDistance;

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
impl EuclideanDistance<PointArray<2>> for PointArray<2> {
    /// Minimum distance between two Points
    fn euclidean_distance(&self, other: &PointArray<2>) -> Float64Array {
        assert_eq!(self.len(), other.len());
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo().zip(other.iter_geo()).for_each(|(first, second)| match (first, second) {
            (Some(first), Some(second)) => output_array.append_value(first.euclidean_distance(&second)),
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

                self.iter_geo().zip(other.iter_geo()).for_each(|(first, second)| match (first, second) {
                    (Some(first), Some(second)) => output_array.append_value(first.euclidean_distance(&second)),
                    _ => output_array.append_null(),
                });

                output_array.finish()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(PointArray<2>, LineStringArray<2>);
iter_geo_impl!(PointArray<2>, PolygonArray<2>);
iter_geo_impl!(PointArray<2>, MultiPointArray<2>);
iter_geo_impl!(PointArray<2>, MultiLineStringArray<2>);
iter_geo_impl!(PointArray<2>, MultiPolygonArray<2>);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray<2>, PointArray<2>);
iter_geo_impl!(LineStringArray<2>, LineStringArray<2>);
iter_geo_impl!(LineStringArray<2>, PolygonArray<2>);
// iter_geo_impl!(LineStringArray<2>, MultiPointArray<2>);
// iter_geo_impl!(LineStringArray<2>, MultiLineStringArray<2>);
// iter_geo_impl!(LineStringArray<2>, MultiPolygonArray<2>);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray<2>, PointArray<2>);
iter_geo_impl!(PolygonArray<2>, LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>, PolygonArray<2>);
// iter_geo_impl!(PolygonArray<2>, MultiPointArray<2>);
// iter_geo_impl!(PolygonArray<2>, MultiLineStringArray<2>);
// iter_geo_impl!(PolygonArray<2>, MultiPolygonArray<2>);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray<2>, PointArray<2>);
// iter_geo_impl!(MultiPointArray<2>, LineStringArray<2>);
// iter_geo_impl!(MultiPointArray<2>, PolygonArray<2>);
// iter_geo_impl!(MultiPointArray<2>, MultiPointArray<2>);
// iter_geo_impl!(MultiPointArray<2>, MultiLineStringArray<2>);
// iter_geo_impl!(MultiPointArray<2>, MultiPolygonArray<2>);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray<2>, PointArray<2>);
// iter_geo_impl!(MultiLineStringArray<2>, LineStringArray<2>);
// iter_geo_impl!(MultiLineStringArray<2>, PolygonArray<2>);
// iter_geo_impl!(MultiLineStringArray<2>, MultiPointArray<2>);
// iter_geo_impl!(MultiLineStringArray<2>, MultiLineStringArray<2>);
// iter_geo_impl!(MultiLineStringArray<2>, MultiPolygonArray<2>);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray<2>, PointArray<2>);
// iter_geo_impl!(MultiPolygonArray<2>, LineStringArray<2>);
// iter_geo_impl!(MultiPolygonArray<2>, PolygonArray<2>);
// iter_geo_impl!(MultiPolygonArray<2>, MultiPointArray<2>);
// iter_geo_impl!(MultiPolygonArray<2>, MultiLineStringArray<2>);
// iter_geo_impl!(MultiPolygonArray<2>, MultiPolygonArray<2>);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl<'a> EuclideanDistance<Point<'a, 2>> for PointArray<2> {
    /// Minimum distance between two Points
    fn euclidean_distance(&self, other: &Point<'a, 2>) -> Float64Array {
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_point| {
            let output = maybe_point.map(|point| point.euclidean_distance(&other.to_geo()));
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
                    let output = maybe_geom.map(|geom| geom.euclidean_distance(&other_geo));
                    output_array.append_option(output)
                });

                output_array.finish()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl_scalar!(PointArray<2>, LineString<'a, 2>);
iter_geo_impl_scalar!(PointArray<2>, Polygon<'a, 2>);
iter_geo_impl_scalar!(PointArray<2>, MultiPoint<'a, 2>);
iter_geo_impl_scalar!(PointArray<2>, MultiLineString<'a, 2>);
iter_geo_impl_scalar!(PointArray<2>, MultiPolygon<'a, 2>);

// Implementations on LineStringArray
iter_geo_impl_scalar!(LineStringArray<2>, Point<'a, 2>);
iter_geo_impl_scalar!(LineStringArray<2>, LineString<'a, 2>);
iter_geo_impl_scalar!(LineStringArray<2>, Polygon<'a, 2>);
// iter_geo_impl_scalar!(LineStringArray<2>, MultiPoint<'a, 2>);
// iter_geo_impl_scalar!(LineStringArray<2>, MultiLineString<'a, 2>);
// iter_geo_impl_scalar!(LineStringArray<2>, MultiPolygon<'a, 2>);

// Implementations on PolygonArray
iter_geo_impl_scalar!(PolygonArray<2>, Point<'a, 2>);
iter_geo_impl_scalar!(PolygonArray<2>, LineString<'a, 2>);
iter_geo_impl_scalar!(PolygonArray<2>, Polygon<'a, 2>);
// iter_geo_impl_scalar!(PolygonArray<2>, MultiPoint<'a, 2>);
// iter_geo_impl_scalar!(PolygonArray<2>, MultiLineString<'a, 2>);
// iter_geo_impl_scalar!(PolygonArray<2>, MultiPolygon<'a, 2>);

// Implementations on MultiPointArray
iter_geo_impl_scalar!(MultiPointArray<2>, Point<'a, 2>);
// iter_geo_impl_scalar!(MultiPointArray<2>, LineString<'a, 2>);
// iter_geo_impl_scalar!(MultiPointArray<2>, Polygon<'a, 2>);
// iter_geo_impl_scalar!(MultiPointArray<2>, MultiPoint<'a, 2>);
// iter_geo_impl_scalar!(MultiPointArray<2>, MultiLineString<'a, 2>);
// iter_geo_impl_scalar!(MultiPointArray<2>, MultiPolygon<'a, 2>);

// Implementations on MultiLineStringArray
iter_geo_impl_scalar!(MultiLineStringArray<2>, Point<'a, 2>);
// iter_geo_impl_scalar!(MultiLineStringArray<2>, LineString<'a, 2>);
// iter_geo_impl_scalar!(MultiLineStringArray<2>, Polygon<'a, 2>);
// iter_geo_impl_scalar!(MultiLineStringArray<2>, MultiPoint<'a, 2>);
// iter_geo_impl_scalar!(MultiLineStringArray<2>, MultiLineString<'a, 2>);
// iter_geo_impl_scalar!(MultiLineStringArray<2>, MultiPolygon<'a, 2>);

// Implementations on MultiPolygonArray
iter_geo_impl_scalar!(MultiPolygonArray<2>, Point<'a, 2>);
// iter_geo_impl_scalar!(MultiPolygonArray<2>, LineString<'a, 2>);
// iter_geo_impl_scalar!(MultiPolygonArray<2>, Polygon<'a, 2>);
// iter_geo_impl_scalar!(MultiPolygonArray<2>, MultiPoint<'a, 2>);
// iter_geo_impl_scalar!(MultiPolygonArray<2>, MultiLineString<'a, 2>);
// iter_geo_impl_scalar!(MultiPolygonArray<2>, MultiPolygon<'a, 2>);
