use crate::array::*;
use crate::scalar::*;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
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
/// `Within` is equivalent to [`Contains`] with the arguments swapped.
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
impl Within for PointArray {
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
iter_geo_impl!(PointArray, LineStringArray<O>);
iter_geo_impl!(PointArray, PolygonArray<O>);
iter_geo_impl!(PointArray, MultiPointArray<O>);
iter_geo_impl!(PointArray, MultiLineStringArray<O>);
iter_geo_impl!(PointArray, MultiPolygonArray<O>);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray<O>, PointArray);
iter_geo_impl!(LineStringArray<O>, LineStringArray<O>);
iter_geo_impl!(LineStringArray<O>, PolygonArray<O>);
iter_geo_impl!(LineStringArray<O>, MultiPointArray<O>);
iter_geo_impl!(LineStringArray<O>, MultiLineStringArray<O>);
iter_geo_impl!(LineStringArray<O>, MultiPolygonArray<O>);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray<O>, PointArray);
iter_geo_impl!(PolygonArray<O>, LineStringArray<O>);
iter_geo_impl!(PolygonArray<O>, PolygonArray<O>);
iter_geo_impl!(PolygonArray<O>, MultiPointArray<O>);
iter_geo_impl!(PolygonArray<O>, MultiLineStringArray<O>);
iter_geo_impl!(PolygonArray<O>, MultiPolygonArray<O>);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray<O>, PointArray);
iter_geo_impl!(MultiPointArray<O>, LineStringArray<O>);
iter_geo_impl!(MultiPointArray<O>, PolygonArray<O>);
iter_geo_impl!(MultiPointArray<O>, MultiPointArray<O>);
iter_geo_impl!(MultiPointArray<O>, MultiLineStringArray<O>);
iter_geo_impl!(MultiPointArray<O>, MultiPolygonArray<O>);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray<O>, PointArray);
iter_geo_impl!(MultiLineStringArray<O>, LineStringArray<O>);
iter_geo_impl!(MultiLineStringArray<O>, PolygonArray<O>);
iter_geo_impl!(MultiLineStringArray<O>, MultiPointArray<O>);
iter_geo_impl!(MultiLineStringArray<O>, MultiLineStringArray<O>);
iter_geo_impl!(MultiLineStringArray<O>, MultiPolygonArray<O>);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray<O>, PointArray);
iter_geo_impl!(MultiPolygonArray<O>, LineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>, PolygonArray<O>);
iter_geo_impl!(MultiPolygonArray<O>, MultiPointArray<O>);
iter_geo_impl!(MultiPolygonArray<O>, MultiLineStringArray<O>);
iter_geo_impl!(MultiPolygonArray<O>, MultiPolygonArray<O>);

// ┌──────────────────────────────────────────┐
// │ Implementations for RHS geoarrow scalars │
// └──────────────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl<'a> Within<Point<'a>> for PointArray {
    fn is_within(&self, rhs: &Point<'a>) -> BooleanArray {
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
iter_geo_impl_geoarrow_scalar!(PointArray, LineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(PointArray, Polygon<'a, O>);
iter_geo_impl_geoarrow_scalar!(PointArray, MultiPoint<'a, O>);
iter_geo_impl_geoarrow_scalar!(PointArray, MultiLineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(PointArray, MultiPolygon<'a, O>);

// Implementations on LineStringArray
iter_geo_impl_geoarrow_scalar!(LineStringArray<O>, Point<'a>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O>, LineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O>, Polygon<'a, O>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O>, MultiPoint<'a, O>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O>, MultiLineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(LineStringArray<O>, MultiPolygon<'a, O>);

// Implementations on PolygonArray
iter_geo_impl_geoarrow_scalar!(PolygonArray<O>, Point<'a>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O>, LineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O>, Polygon<'a, O>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O>, MultiPoint<'a, O>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O>, MultiLineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(PolygonArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiPointArray
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O>, Point<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O>, LineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O>, Polygon<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O>, MultiPoint<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O>, MultiLineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiLineStringArray
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O>, Point<'a>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O>, LineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O>, Polygon<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O>, MultiPoint<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O>, MultiLineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiPolygonArray
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O>, Point<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O>, LineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O>, Polygon<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O>, MultiPoint<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O>, MultiLineString<'a, O>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray<O>, MultiPolygon<'a, O>);

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
non_generic_iter_geo_impl_geo_scalar!(PointArray, geo::Point);
non_generic_iter_geo_impl_geo_scalar!(PointArray, geo::LineString);
non_generic_iter_geo_impl_geo_scalar!(PointArray, geo::Polygon);
non_generic_iter_geo_impl_geo_scalar!(PointArray, geo::MultiPoint);
non_generic_iter_geo_impl_geo_scalar!(PointArray, geo::MultiLineString);
non_generic_iter_geo_impl_geo_scalar!(PointArray, geo::MultiPolygon);

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
iter_geo_impl_geo_scalar!(LineStringArray<O>, geo::Point);
iter_geo_impl_geo_scalar!(LineStringArray<O>, geo::LineString);
iter_geo_impl_geo_scalar!(LineStringArray<O>, geo::Polygon);
iter_geo_impl_geo_scalar!(LineStringArray<O>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(LineStringArray<O>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(LineStringArray<O>, geo::MultiPolygon);

// Implementations on PolygonArray
iter_geo_impl_geo_scalar!(PolygonArray<O>, geo::Point);
iter_geo_impl_geo_scalar!(PolygonArray<O>, geo::LineString);
iter_geo_impl_geo_scalar!(PolygonArray<O>, geo::Polygon);
iter_geo_impl_geo_scalar!(PolygonArray<O>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(PolygonArray<O>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(PolygonArray<O>, geo::MultiPolygon);

// Implementations on MultiPointArray
iter_geo_impl_geo_scalar!(MultiPointArray<O>, geo::Point);
iter_geo_impl_geo_scalar!(MultiPointArray<O>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPointArray<O>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPointArray<O>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPointArray<O>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPointArray<O>, geo::MultiPolygon);

// Implementations on MultiLineStringArray
iter_geo_impl_geo_scalar!(MultiLineStringArray<O>, geo::Point);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray<O>, geo::MultiPolygon);

// Implementations on MultiPolygonArray
iter_geo_impl_geo_scalar!(MultiPolygonArray<O>, geo::Point);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O>, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O>, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O>, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O>, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray<O>, geo::MultiPolygon);
