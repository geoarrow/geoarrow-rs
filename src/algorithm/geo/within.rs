use crate::array::*;
use crate::scalar::*;
use crate::trait_::GeometryArrayAccessor;
use crate::trait_::GeometryScalarTrait;
use crate::NativeArray;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait};
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
        impl<'a, O: OffsetSizeTrait> Within<$second> for $first {
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
iter_geo_impl!(PointArray<2>, LineStringArray<O, 2>);
iter_geo_impl!(PointArray<2>, PolygonArray<O, 2>);
iter_geo_impl!(PointArray<2>, MultiPointArray<O, 2>);
iter_geo_impl!(PointArray<2>, MultiLineStringArray<O, 2>);
iter_geo_impl!(PointArray<2>, MultiPolygonArray<O, 2>);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray<O, 2>, PointArray<2>);
iter_geo_impl!(LineStringArray<O, 2>, LineStringArray<O, 2>);
iter_geo_impl!(LineStringArray<O, 2>, PolygonArray<O, 2>);
iter_geo_impl!(LineStringArray<O, 2>, MultiPointArray<O, 2>);
iter_geo_impl!(LineStringArray<O, 2>, MultiLineStringArray<O, 2>);
iter_geo_impl!(LineStringArray<O, 2>, MultiPolygonArray<O, 2>);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray<O, 2>, PointArray<2>);
iter_geo_impl!(PolygonArray<O, 2>, LineStringArray<O, 2>);
iter_geo_impl!(PolygonArray<O, 2>, PolygonArray<O, 2>);
iter_geo_impl!(PolygonArray<O, 2>, MultiPointArray<O, 2>);
iter_geo_impl!(PolygonArray<O, 2>, MultiLineStringArray<O, 2>);
iter_geo_impl!(PolygonArray<O, 2>, MultiPolygonArray<O, 2>);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray<O, 2>, PointArray<2>);
iter_geo_impl!(MultiPointArray<O, 2>, LineStringArray<O, 2>);
iter_geo_impl!(MultiPointArray<O, 2>, PolygonArray<O, 2>);
iter_geo_impl!(MultiPointArray<O, 2>, MultiPointArray<O, 2>);
iter_geo_impl!(MultiPointArray<O, 2>, MultiLineStringArray<O, 2>);
iter_geo_impl!(MultiPointArray<O, 2>, MultiPolygonArray<O, 2>);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray<O, 2>, PointArray<2>);
iter_geo_impl!(MultiLineStringArray<O, 2>, LineStringArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>, PolygonArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>, MultiPointArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>, MultiLineStringArray<O, 2>);
iter_geo_impl!(MultiLineStringArray<O, 2>, MultiPolygonArray<O, 2>);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray<O, 2>, PointArray<2>);
iter_geo_impl!(MultiPolygonArray<O, 2>, LineStringArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>, PolygonArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>, MultiPointArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>, MultiLineStringArray<O, 2>);
iter_geo_impl!(MultiPolygonArray<O, 2>, MultiPolygonArray<O, 2>);

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
        impl<'a, O: OffsetSizeTrait> Within<$second> for $first {
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
iter_geo_impl_geoarrow_scalar!(PointArray<2>, LineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, Polygon<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, MultiPoint<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, MultiLineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PointArray<2>, MultiPolygon<'a, O, 2>);

// Implementations on LineStringArray
iter_geo_impl_geoarrow_scalar!(LineStringArray<O, 2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O, 2>, LineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O, 2>, Polygon<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O, 2>, MultiPoint<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O, 2>, MultiLineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O, 2>, MultiPolygon<'a, O, 2>);

// Implementations on PolygonArray
iter_geo_impl_geoarrow_scalar!(PolygonArray<O, 2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O, 2>, LineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O, 2>, Polygon<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O, 2>, MultiPoint<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O, 2>, MultiLineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O, 2>, MultiPolygon<'a, O, 2>);

// Implementations on MultiPointArray
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O, 2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O, 2>, LineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O, 2>, Polygon<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O, 2>, MultiPoint<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O, 2>, MultiLineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O, 2>, MultiPolygon<'a, O, 2>);

// Implementations on MultiLineStringArray
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O, 2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O, 2>, LineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O, 2>, Polygon<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O, 2>, MultiPoint<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O, 2>, MultiLineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O, 2>, MultiPolygon<'a, O, 2>);

// Implementations on MultiPolygonArray
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O, 2>, Point<'a, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O, 2>, LineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O, 2>, Polygon<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O, 2>, MultiPoint<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O, 2>, MultiLineString<'a, O, 2>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O, 2>, MultiPolygon<'a, O, 2>);

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
        impl<'a, O: OffsetSizeTrait> Within<$second> for $first {
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
iter_geo_impl_geo_scalar!(LineStringArray<O, 2>, geo::Point);
iter_geo_impl_geo_scalar!(LineStringArray<O, 2>, geo::LineString);
iter_geo_impl_geo_scalar!(LineStringArray<O, 2>, geo::Polygon);
iter_geo_impl_geo_scalar!(LineStringArray<O, 2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(LineStringArray<O, 2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(LineStringArray<O, 2>, geo::MultiPolygon);

// Implementations on PolygonArray
iter_geo_impl_geo_scalar!(PolygonArray<O, 2>, geo::Point);
iter_geo_impl_geo_scalar!(PolygonArray<O, 2>, geo::LineString);
iter_geo_impl_geo_scalar!(PolygonArray<O, 2>, geo::Polygon);
iter_geo_impl_geo_scalar!(PolygonArray<O, 2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(PolygonArray<O, 2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(PolygonArray<O, 2>, geo::MultiPolygon);

// Implementations on MultiPointArray
iter_geo_impl_geo_scalar!(MultiPointArray<O, 2>, geo::Point);
iter_geo_impl_geo_scalar!(MultiPointArray<O, 2>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPointArray<O, 2>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPointArray<O, 2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPointArray<O, 2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPointArray<O, 2>, geo::MultiPolygon);

// Implementations on MultiLineStringArray
iter_geo_impl_geo_scalar!(MultiLineStringArray<O, 2>, geo::Point);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O, 2>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O, 2>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O, 2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O, 2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O, 2>, geo::MultiPolygon);

// Implementations on MultiPolygonArray
iter_geo_impl_geo_scalar!(MultiPolygonArray<O, 2>, geo::Point);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O, 2>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O, 2>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O, 2>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O, 2>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O, 2>, geo::MultiPolygon);
