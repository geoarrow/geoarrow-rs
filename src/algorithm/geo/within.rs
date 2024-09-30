use crate::array::*;
use crate::scalar::*;
use crate::trait_::ArrayAccessor;
use crate::trait_::NativeScalar;
use arrow_array::builder::BooleanBuilder;
use arrow_array::BooleanArray;
use geo::Within as _Within;

/// Tests if a geometry is completely within another geometry.
///
/// In other words, the [DE-9IM] intersection matrix for (Self, Rhs) is `[T*F**F***]`
///
/// # Examples
///
/// ```
/// use geo::{point, line_string};
/// use geo::algorithm::Within;
///
/// let line_string = line_string![(x: 0.0, y: 0.0), (x: 2.0, y: 4.0)];
///
/// assert!(point!(x: 1.0, y: 2.0).is_within(&line_string));
///
/// // Note that a geometry on only the *boundary* of another geometry is not considered to
/// // be _within_ that geometry. See [`Relate`] for more information.
/// assert!(! point!(x: 0.0, y: 0.0).is_within(&line_string));
/// ```
///
/// `Within` is equivalent to [`Contains`](geo::Contains) with the arguments swapped.
///
/// ```
/// use geo::{point, line_string};
/// use geo::algorithm::{Contains, Within};
///
/// let line_string = line_string![(x: 0.0, y: 0.0), (x: 2.0, y: 4.0)];
/// let point = point!(x: 1.0, y: 2.0);
///
/// // These two comparisons are completely equivalent
/// assert!(point.is_within(&line_string));
/// assert!(line_string.contains(&point));
/// ```
///
/// [DE-9IM]: https://en.wikipedia.org/wiki/DE-9IM
pub trait Within<Other = Self> {
    fn is_within(&self, b: &Other) -> BooleanArray;
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl Within for PointArray<2> {
    fn is_within(&self, rhs: &Self) -> BooleanArray {
        assert_eq!(self.len(), rhs.len());

        let mut output_array = BooleanBuilder::with_capacity(self.len());

        self.iter_geo()
            .zip(rhs.iter_geo())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(second)) => output_array.append_value(first.is_within(&second)),
                _ => output_array.append_null(),
            });

        output_array.finish()
    }
}

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a> Within<$second> for $first {
            fn is_within(&self, rhs: &$second) -> BooleanArray {
                assert_eq!(self.len(), rhs.len());

                let mut output_array = BooleanBuilder::with_capacity(self.len());

                self.iter_geo()
                    .zip(rhs.iter_geo())
                    .for_each(|(first, second)| match (first, second) {
                        (Some(first), Some(second)) => {
                            output_array.append_value(first.is_within(&second))
                        }
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
iter_geo_impl!(LineStringArray<2>, MultiPointArray<2>);
iter_geo_impl!(LineStringArray<2>, MultiLineStringArray<2>);
iter_geo_impl!(LineStringArray<2>, MultiPolygonArray<2>);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray<2>, PointArray<2>);
iter_geo_impl!(PolygonArray<2>, LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>, PolygonArray<2>);
iter_geo_impl!(PolygonArray<2>, MultiPointArray<2>);
iter_geo_impl!(PolygonArray<2>, MultiLineStringArray<2>);
iter_geo_impl!(PolygonArray<2>, MultiPolygonArray<2>);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray<2>, PointArray<2>);
iter_geo_impl!(MultiPointArray<2>, LineStringArray<2>);
iter_geo_impl!(MultiPointArray<2>, PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>, MultiPointArray<2>);
iter_geo_impl!(MultiPointArray<2>, MultiLineStringArray<2>);
iter_geo_impl!(MultiPointArray<2>, MultiPolygonArray<2>);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray<2>, PointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>, LineStringArray<2>);
iter_geo_impl!(MultiLineStringArray<2>, PolygonArray<2>);
iter_geo_impl!(MultiLineStringArray<2>, MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>, MultiLineStringArray<2>);
iter_geo_impl!(MultiLineStringArray<2>, MultiPolygonArray<2>);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray<2>, PointArray<2>);
iter_geo_impl!(MultiPolygonArray<2>, LineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>, PolygonArray<2>);
iter_geo_impl!(MultiPolygonArray<2>, MultiPointArray<2>);
iter_geo_impl!(MultiPolygonArray<2>, MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>, MultiPolygonArray<2>);

// ┌──────────────────────────────────────────┐
// │ Implementations for RHS geoarrow scalars │
// └──────────────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl<'a> Within<Point<'a, 2>> for PointArray<2> {
    fn is_within(&self, rhs: &Point<'a, 2>) -> BooleanArray {
        let mut output_array = BooleanBuilder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_point| {
            let output = maybe_point.map(|point| point.is_within(&rhs.to_geo()));
            output_array.append_option(output)
        });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl_geoarrow_scalar {
    ($first:ty, $second:ty) => {
        impl<'a> Within<$second> for $first {
            fn is_within(&self, rhs: &$second) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());
                let rhs_geo = rhs.to_geo();

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| geom.is_within(&rhs_geo));
                    output_array.append_option(output)
                });

                output_array.finish()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl_geoarrow_scalar!(PointArray<2>, LineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, Polygon<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, MultiPoint<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, MultiLineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, MultiPolygon<'a, 2>);

// Implementations on LineStringArray
iter_geo_impl_geoarrow_scalar!(LineStringArray<2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<2>, LineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<2>, Polygon<'a, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<2>, MultiPoint<'a, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<2>, MultiLineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<2>, MultiPolygon<'a, 2>);

// Implementations on PolygonArray
iter_geo_impl_geoarrow_scalar!(PolygonArray<2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<2>, LineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<2>, Polygon<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<2>, MultiPoint<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<2>, MultiLineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<2>, MultiPolygon<'a, 2>);

// Implementations on MultiPointArray
iter_geo_impl_geoarrow_scalar!(MultiPointArray<2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<2>, LineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<2>, Polygon<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<2>, MultiPoint<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<2>, MultiLineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<2>, MultiPolygon<'a, 2>);

// Implementations on MultiLineStringArray
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<2>, LineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<2>, Polygon<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<2>, MultiPoint<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<2>, MultiLineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<2>, MultiPolygon<'a, 2>);

// Implementations on MultiPolygonArray
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<2>, LineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<2>, Polygon<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<2>, MultiPoint<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<2>, MultiLineString<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<2>, MultiPolygon<'a, 2>);

// ┌─────────────────────────────────────┐
// │ Implementations for RHS geo scalars │
// └─────────────────────────────────────┘

macro_rules! non_generic_iter_geo_impl_geo_scalar {
    ($first:ty, $second:ty) => {
        impl<'a> Within<$second> for $first {
            fn is_within(&self, rhs: &$second) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| geom.is_within(rhs));
                    output_array.append_option(output)
                });

                output_array.finish()
            }
        }
    };
}

// Implementations on PointArray
non_generic_iter_geo_impl_geo_scalar!(PointArray<2>, geo::Point);
non_generic_iter_geo_impl_geo_scalar!(PointArray<2>, geo::LineString);
non_generic_iter_geo_impl_geo_scalar!(PointArray<2>, geo::Polygon);
non_generic_iter_geo_impl_geo_scalar!(PointArray<2>, geo::MultiPoint);
non_generic_iter_geo_impl_geo_scalar!(PointArray<2>, geo::MultiLineString);
non_generic_iter_geo_impl_geo_scalar!(PointArray<2>, geo::MultiPolygon);

macro_rules! iter_geo_impl_geo_scalar {
    ($first:ty, $second:ty) => {
        impl<'a> Within<$second> for $first {
            fn is_within(&self, rhs: &$second) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| geom.is_within(rhs));
                    output_array.append_option(output)
                });

                output_array.finish()
            }
        }
    };
}

// Implementations on LineStringArray
iter_geo_impl_geo_scalar!(LineStringArray<2>, geo::Point);
iter_geo_impl_geo_scalar!(LineStringArray<2>, geo::LineString);
iter_geo_impl_geo_scalar!(LineStringArray<2>, geo::Polygon);
iter_geo_impl_geo_scalar!(LineStringArray<2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(LineStringArray<2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(LineStringArray<2>, geo::MultiPolygon);

// Implementations on PolygonArray
iter_geo_impl_geo_scalar!(PolygonArray<2>, geo::Point);
iter_geo_impl_geo_scalar!(PolygonArray<2>, geo::LineString);
iter_geo_impl_geo_scalar!(PolygonArray<2>, geo::Polygon);
iter_geo_impl_geo_scalar!(PolygonArray<2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(PolygonArray<2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(PolygonArray<2>, geo::MultiPolygon);

// Implementations on MultiPointArray
iter_geo_impl_geo_scalar!(MultiPointArray<2>, geo::Point);
iter_geo_impl_geo_scalar!(MultiPointArray<2>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPointArray<2>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPointArray<2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPointArray<2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPointArray<2>, geo::MultiPolygon);

// Implementations on MultiLineStringArray
iter_geo_impl_geo_scalar!(MultiLineStringArray<2>, geo::Point);
iter_geo_impl_geo_scalar!(MultiLineStringArray<2>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray<2>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiLineStringArray<2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiLineStringArray<2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray<2>, geo::MultiPolygon);

// Implementations on MultiPolygonArray
iter_geo_impl_geo_scalar!(MultiPolygonArray<2>, geo::Point);
iter_geo_impl_geo_scalar!(MultiPolygonArray<2>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray<2>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPolygonArray<2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPolygonArray<2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray<2>, geo::MultiPolygon);
