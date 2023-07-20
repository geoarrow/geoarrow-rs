use crate::algorithm::broadcasting::*;
use crate::array::*;
use crate::trait_::{GeometryArrayTrait, GeometryScalarTrait};
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow2::types::Offset;
use geo::EuclideanDistance as _EuclideanDistance;

pub trait EuclideanDistance<Rhs = Self> {
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
    fn euclidean_distance(&self, rhs: &Rhs) -> PrimitiveArray<f64>;
}

// ┌────────────────────────────────┐
// │ Implementations for PointArray │
// └────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl<'a> EuclideanDistance<BroadcastablePoint<'a>> for PointArray {
    /// Minimum distance between two Points
    fn euclidean_distance(&self, other: &BroadcastablePoint) -> PrimitiveArray<f64> {
        // assert_eq!(self.len(), other.len());
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());

        self.iter_geo()
            .zip(other.into_iter())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), second) => {
                    output_array.push(Some(first.euclidean_distance(&second.to_geo())))
                }
                _ => output_array.push(None),
            });

        output_array.into()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a, O: Offset> EuclideanDistance<$second> for $first {
            fn euclidean_distance(&self, other: &$second) -> PrimitiveArray<f64> {
                // assert_eq!(self.len(), other.len());
                let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());

                self.iter_geo()
                    .zip(other.into_iter())
                    .for_each(|(first, second)| match (first, second) {
                        (Some(first), second) => {
                            output_array.push(Some(first.euclidean_distance(&second.to_geo())))
                        }
                        _ => output_array.push(None),
                    });

                output_array.into()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(PointArray, BroadcastableLineString<'a, O>);
iter_geo_impl!(PointArray, BroadcastablePolygon<'a, O>);
iter_geo_impl!(PointArray, BroadcastableMultiPoint<'a, O>);
iter_geo_impl!(PointArray, BroadcastableMultiLineString<'a, O>);
iter_geo_impl!(PointArray, BroadcastableMultiPolygon<'a, O>);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray<O>, BroadcastablePoint<'a>);
iter_geo_impl!(LineStringArray<O>, BroadcastableLineString<'a, O>);
iter_geo_impl!(LineStringArray<O>, BroadcastablePolygon<'a, O>);
// iter_geo_impl!(LineStringArray<O>, BroadcastableMultiPoint<'a, O>);
// iter_geo_impl!(LineStringArray<O>, BroadcastableMultiLineString<'a, O>);
// iter_geo_impl!(LineStringArray<O>, BroadcastableMultiPolygon<'a, O>);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray<O>, BroadcastablePoint<'a>);
iter_geo_impl!(PolygonArray<O>, BroadcastableLineString<'a, O>);
iter_geo_impl!(PolygonArray<O>, BroadcastablePolygon<'a, O>);
// iter_geo_impl!(PolygonArray<O>, BroadcastableMultiPoint<'a, O>);
// iter_geo_impl!(PolygonArray<O>, BroadcastableMultiLineString<'a, O>);
// iter_geo_impl!(PolygonArray<O>, BroadcastableMultiPolygon<'a, O>);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray<O>, BroadcastablePoint<'a>);
// iter_geo_impl!(MultiPointArray<O>, BroadcastableLineString<'a, O>);
// iter_geo_impl!(MultiPointArray<O>, BroadcastablePolygon<'a, O>);
// iter_geo_impl!(MultiPointArray<O>, BroadcastableMultiPoint<'a, O>);
// iter_geo_impl!(MultiPointArray<O>, BroadcastableMultiLineString<'a, O>);
// iter_geo_impl!(MultiPointArray<O>, BroadcastableMultiPolygon<'a, O>);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray<O>, BroadcastablePoint<'a>);
// iter_geo_impl!(MultiLineStringArray<O>, BroadcastableLineString<'a, O>);
// iter_geo_impl!(MultiLineStringArray<O>, BroadcastablePolygon<'a, O>);
// iter_geo_impl!(MultiLineStringArray<O>, BroadcastableMultiPoint<'a, O>);
// iter_geo_impl!(MultiLineStringArray<O>, BroadcastableMultiLineString<'a, O>);
// iter_geo_impl!(MultiLineStringArray<O>, BroadcastableMultiPolygon<'a, O>);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray<O>, BroadcastablePoint<'a>);
// iter_geo_impl!(MultiPolygonArray<O>, BroadcastableLineString<'a, O>);
// iter_geo_impl!(MultiPolygonArray<O>, BroadcastablePolygon<'a, O>);
// iter_geo_impl!(MultiPolygonArray<O>, BroadcastableMultiPoint<'a, O>);
// iter_geo_impl!(MultiPolygonArray<O>, BroadcastableMultiLineString<'a, O>);
// iter_geo_impl!(MultiPolygonArray<O>, BroadcastableMultiPolygon<'a, O>);
