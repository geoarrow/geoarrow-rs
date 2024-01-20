use crate::array::*;
use crate::scalar::*;
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::{GeometryArrayTrait, GeometryScalarTrait};
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
impl EuclideanDistance<PointArray> for PointArray {
    /// Minimum distance between two Points
    fn euclidean_distance(&self, other: &PointArray) -> Float64Array {
        assert_eq!(self.len(), other.len());
        let mut output_array = Float64Builder::with_capacity(self.len());

        self.iter_geo()
            .zip(other.iter_geo())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(second)) => {
                    output_array.append_value(first.euclidean_distance(&second))
                }
                _ => output_array.append_null(),
            });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a, O: OffsetSizeTrait> EuclideanDistance<$second> for $first {
            fn euclidean_distance(&self, other: &$second) -> Float64Array {
                assert_eq!(self.len(), other.len());
                let mut output_array = Float64Builder::with_capacity(self.len());

                self.iter_geo()
                    .zip(other.iter_geo())
                    .for_each(|(first, second)| match (first, second) {
                        (Some(first), Some(second)) => {
                            output_array.append_value(first.euclidean_distance(&second))
                        }
                        _ => output_array.append_null(),
                    });

                output_array.finish()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(PointArray, LineStringArray<O>);
iter_geo_impl!(PointArray, PolygonArray<O>);
iter_geo_impl!(PointArray, MultiPointArray<O>);
iter_geo_impl!(PointArray, MultiLineStringArray<O>);
iter_geo_impl!(PointArray, MultiPolygonArray<O>);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray<O>, PointArray);
iter_geo_impl!(LineStringArray<O>, LineStringArray<O>);
iter_geo_impl!(LineStringArray<O>, PolygonArray<O>);
// iter_geo_impl!(LineStringArray<O>, MultiPointArray<O>);
// iter_geo_impl!(LineStringArray<O>, MultiLineStringArray<O>);
// iter_geo_impl!(LineStringArray<O>, MultiPolygonArray<O>);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray<O>, PointArray);
iter_geo_impl!(PolygonArray<O>, LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>, PolygonArray<O>);
// iter_geo_impl!(PolygonArray<O>, MultiPointArray<O>);
// iter_geo_impl!(PolygonArray<O>, MultiLineStringArray<O>);
// iter_geo_impl!(PolygonArray<O>, MultiPolygonArray<O>);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray<O>, PointArray);
// iter_geo_impl!(MultiPointArray<O>, LineStringArray<O>);
// iter_geo_impl!(MultiPointArray<O>, PolygonArray<O>);
// iter_geo_impl!(MultiPointArray<O>, MultiPointArray<O>);
// iter_geo_impl!(MultiPointArray<O>, MultiLineStringArray<O>);
// iter_geo_impl!(MultiPointArray<O>, MultiPolygonArray<O>);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray<O>, PointArray);
// iter_geo_impl!(MultiLineStringArray<O>, LineStringArray<O>);
// iter_geo_impl!(MultiLineStringArray<O>, PolygonArray<O>);
// iter_geo_impl!(MultiLineStringArray<O>, MultiPointArray<O>);
// iter_geo_impl!(MultiLineStringArray<O>, MultiLineStringArray<O>);
// iter_geo_impl!(MultiLineStringArray<O>, MultiPolygonArray<O>);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray<O>, PointArray);
// iter_geo_impl!(MultiPolygonArray<O>, LineStringArray<O>);
// iter_geo_impl!(MultiPolygonArray<O>, PolygonArray<O>);
// iter_geo_impl!(MultiPolygonArray<O>, MultiPointArray<O>);
// iter_geo_impl!(MultiPolygonArray<O>, MultiLineStringArray<O>);
// iter_geo_impl!(MultiPolygonArray<O>, MultiPolygonArray<O>);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl<'a> EuclideanDistance<Point<'a>> for PointArray {
    /// Minimum distance between two Points
    fn euclidean_distance(&self, other: &Point<'a>) -> Float64Array {
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
        impl<'a, O: OffsetSizeTrait> EuclideanDistance<$second> for $first {
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
iter_geo_impl_scalar!(PointArray, LineString<'a, O>);
iter_geo_impl_scalar!(PointArray, Polygon<'a, O>);
iter_geo_impl_scalar!(PointArray, MultiPoint<'a, O>);
iter_geo_impl_scalar!(PointArray, MultiLineString<'a, O>);
iter_geo_impl_scalar!(PointArray, MultiPolygon<'a, O>);

// Implementations on LineStringArray
iter_geo_impl_scalar!(LineStringArray<O>, Point<'a>);
iter_geo_impl_scalar!(LineStringArray<O>, LineString<'a, O>);
iter_geo_impl_scalar!(LineStringArray<O>, Polygon<'a, O>);
// iter_geo_impl_scalar!(LineStringArray<O>, MultiPoint<'a, O>);
// iter_geo_impl_scalar!(LineStringArray<O>, MultiLineString<'a, O>);
// iter_geo_impl_scalar!(LineStringArray<O>, MultiPolygon<'a, O>);

// Implementations on PolygonArray
iter_geo_impl_scalar!(PolygonArray<O>, Point<'a>);
iter_geo_impl_scalar!(PolygonArray<O>, LineString<'a, O>);
iter_geo_impl_scalar!(PolygonArray<O>, Polygon<'a, O>);
// iter_geo_impl_scalar!(PolygonArray<O>, MultiPoint<'a, O>);
// iter_geo_impl_scalar!(PolygonArray<O>, MultiLineString<'a, O>);
// iter_geo_impl_scalar!(PolygonArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiPointArray
iter_geo_impl_scalar!(MultiPointArray<O>, Point<'a>);
// iter_geo_impl_scalar!(MultiPointArray<O>, LineString<'a, O>);
// iter_geo_impl_scalar!(MultiPointArray<O>, Polygon<'a, O>);
// iter_geo_impl_scalar!(MultiPointArray<O>, MultiPoint<'a, O>);
// iter_geo_impl_scalar!(MultiPointArray<O>, MultiLineString<'a, O>);
// iter_geo_impl_scalar!(MultiPointArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiLineStringArray
iter_geo_impl_scalar!(MultiLineStringArray<O>, Point<'a>);
// iter_geo_impl_scalar!(MultiLineStringArray<O>, LineString<'a, O>);
// iter_geo_impl_scalar!(MultiLineStringArray<O>, Polygon<'a, O>);
// iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiPoint<'a, O>);
// iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiLineString<'a, O>);
// iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiPolygonArray
iter_geo_impl_scalar!(MultiPolygonArray<O>, Point<'a>);
// iter_geo_impl_scalar!(MultiPolygonArray<O>, LineString<'a, O>);
// iter_geo_impl_scalar!(MultiPolygonArray<O>, Polygon<'a, O>);
// iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiPoint<'a, O>);
// iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiLineString<'a, O>);
// iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiPolygon<'a, O>);
