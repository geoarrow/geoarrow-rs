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
iter_geo_impl!(PointArray, LineStringArray);
iter_geo_impl!(PointArray, PolygonArray);
iter_geo_impl!(PointArray, MultiPointArray);
iter_geo_impl!(PointArray, MultiLineStringArray);
iter_geo_impl!(PointArray, MultiPolygonArray);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray, PointArray);
iter_geo_impl!(LineStringArray, LineStringArray);
iter_geo_impl!(LineStringArray, PolygonArray);
iter_geo_impl!(LineStringArray, MultiPointArray);
iter_geo_impl!(LineStringArray, MultiLineStringArray);
iter_geo_impl!(LineStringArray, MultiPolygonArray);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray, PointArray);
iter_geo_impl!(PolygonArray, LineStringArray);
iter_geo_impl!(PolygonArray, PolygonArray);
iter_geo_impl!(PolygonArray, MultiPointArray);
iter_geo_impl!(PolygonArray, MultiLineStringArray);
iter_geo_impl!(PolygonArray, MultiPolygonArray);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray, PointArray);
iter_geo_impl!(MultiPointArray, LineStringArray);
iter_geo_impl!(MultiPointArray, PolygonArray);
iter_geo_impl!(MultiPointArray, MultiPointArray);
iter_geo_impl!(MultiPointArray, MultiLineStringArray);
iter_geo_impl!(MultiPointArray, MultiPolygonArray);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray, PointArray);
iter_geo_impl!(MultiLineStringArray, LineStringArray);
iter_geo_impl!(MultiLineStringArray, PolygonArray);
iter_geo_impl!(MultiLineStringArray, MultiPointArray);
iter_geo_impl!(MultiLineStringArray, MultiLineStringArray);
iter_geo_impl!(MultiLineStringArray, MultiPolygonArray);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray, PointArray);
iter_geo_impl!(MultiPolygonArray, LineStringArray);
iter_geo_impl!(MultiPolygonArray, PolygonArray);
iter_geo_impl!(MultiPolygonArray, MultiPointArray);
iter_geo_impl!(MultiPolygonArray, MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray, MultiPolygonArray);

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
iter_geo_impl_geoarrow_scalar!(PointArray, LineString<'a>);
iter_geo_impl_geoarrow_scalar!(PointArray, Polygon<'a>);
iter_geo_impl_geoarrow_scalar!(PointArray, MultiPoint<'a>);
iter_geo_impl_geoarrow_scalar!(PointArray, MultiLineString<'a>);
iter_geo_impl_geoarrow_scalar!(PointArray, MultiPolygon<'a>);

// Implementations on LineStringArray
iter_geo_impl_geoarrow_scalar!(LineStringArray, Point<'a>);
iter_geo_impl_geoarrow_scalar!(LineStringArray, LineString<'a>);
iter_geo_impl_geoarrow_scalar!(LineStringArray, Polygon<'a>);
iter_geo_impl_geoarrow_scalar!(LineStringArray, MultiPoint<'a>);
iter_geo_impl_geoarrow_scalar!(LineStringArray, MultiLineString<'a>);
iter_geo_impl_geoarrow_scalar!(LineStringArray, MultiPolygon<'a>);

// Implementations on PolygonArray
iter_geo_impl_geoarrow_scalar!(PolygonArray, Point<'a>);
iter_geo_impl_geoarrow_scalar!(PolygonArray, LineString<'a>);
iter_geo_impl_geoarrow_scalar!(PolygonArray, Polygon<'a>);
iter_geo_impl_geoarrow_scalar!(PolygonArray, MultiPoint<'a>);
iter_geo_impl_geoarrow_scalar!(PolygonArray, MultiLineString<'a>);
iter_geo_impl_geoarrow_scalar!(PolygonArray, MultiPolygon<'a>);

// Implementations on MultiPointArray
iter_geo_impl_geoarrow_scalar!(MultiPointArray, Point<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray, LineString<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray, Polygon<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray, MultiPoint<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray, MultiLineString<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPointArray, MultiPolygon<'a>);

// Implementations on MultiLineStringArray
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray, Point<'a>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray, LineString<'a>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray, Polygon<'a>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray, MultiPoint<'a>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray, MultiLineString<'a>);
iter_geo_impl_geoarrow_scalar!(MultiLineStringArray, MultiPolygon<'a>);

// Implementations on MultiPolygonArray
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray, Point<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray, LineString<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray, Polygon<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray, MultiPoint<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray, MultiLineString<'a>);
iter_geo_impl_geoarrow_scalar!(MultiPolygonArray, MultiPolygon<'a>);

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
iter_geo_impl_geo_scalar!(LineStringArray, geo::Point);
iter_geo_impl_geo_scalar!(LineStringArray, geo::LineString);
iter_geo_impl_geo_scalar!(LineStringArray, geo::Polygon);
iter_geo_impl_geo_scalar!(LineStringArray, geo::MultiPoint);
iter_geo_impl_geo_scalar!(LineStringArray, geo::MultiLineString);
iter_geo_impl_geo_scalar!(LineStringArray, geo::MultiPolygon);

// Implementations on PolygonArray
iter_geo_impl_geo_scalar!(PolygonArray, geo::Point);
iter_geo_impl_geo_scalar!(PolygonArray, geo::LineString);
iter_geo_impl_geo_scalar!(PolygonArray, geo::Polygon);
iter_geo_impl_geo_scalar!(PolygonArray, geo::MultiPoint);
iter_geo_impl_geo_scalar!(PolygonArray, geo::MultiLineString);
iter_geo_impl_geo_scalar!(PolygonArray, geo::MultiPolygon);

// Implementations on MultiPointArray
iter_geo_impl_geo_scalar!(MultiPointArray, geo::Point);
iter_geo_impl_geo_scalar!(MultiPointArray, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPointArray, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPointArray, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPointArray, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPointArray, geo::MultiPolygon);

// Implementations on MultiLineStringArray
iter_geo_impl_geo_scalar!(MultiLineStringArray, geo::Point);
iter_geo_impl_geo_scalar!(MultiLineStringArray, geo::LineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiLineStringArray, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiLineStringArray, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiLineStringArray, geo::MultiPolygon);

// Implementations on MultiPolygonArray
iter_geo_impl_geo_scalar!(MultiPolygonArray, geo::Point);
iter_geo_impl_geo_scalar!(MultiPolygonArray, geo::LineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray, geo::Polygon);
iter_geo_impl_geo_scalar!(MultiPolygonArray, geo::MultiPoint);
iter_geo_impl_geo_scalar!(MultiPolygonArray, geo::MultiLineString);
iter_geo_impl_geo_scalar!(MultiPolygonArray, geo::MultiPolygon);
