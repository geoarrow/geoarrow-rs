use crate::array::*;
use crate::scalar::*;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait};
use geo::Contains as _Contains;

/// Checks if `rhs` is completely contained within `self`.
/// More formally, the interior of `rhs` has non-empty
/// (set-theoretic) intersection but neither the interior,
/// nor the boundary of `rhs` intersects the exterior of
/// `self`. In other words, the [DE-9IM] intersection matrix
/// of `(rhs, self)` is `T*F**F***`.
///
/// [DE-9IM]: https://en.wikipedia.org/wiki/DE-9IM
///
/// # Examples
///
/// ```
/// use geo::Contains;
/// use geo::{line_string, point, Polygon};
///
/// let line_string = line_string![
///     (x: 0., y: 0.),
///     (x: 2., y: 0.),
///     (x: 2., y: 2.),
///     (x: 0., y: 2.),
///     (x: 0., y: 0.),
/// ];
///
/// let polygon = Polygon::new(line_string.clone(), vec![]);
///
/// // Point in Point
/// assert!(point!(x: 2., y: 0.).contains(&point!(x: 2., y: 0.)));
///
/// // Point in Linestring
/// assert!(line_string.contains(&point!(x: 2., y: 0.)));
///
/// // Point in Polygon
/// assert!(polygon.contains(&point!(x: 1., y: 1.)));
/// ```
pub trait Contains<Rhs = Self> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl Contains for PointArray {
    fn contains(&self, rhs: &Self) -> BooleanArray {
        assert_eq!(self.len(), rhs.len());

        let mut output_array = BooleanBuilder::with_capacity(self.len());

        self.iter_geo()
            .zip(rhs.iter_geo())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(second)) => output_array.append_value(first.contains(&second)),
                _ => output_array.append_null(),
            });

        output_array.finish()
    }
}

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a, O: OffsetSizeTrait> Contains<$second> for $first {
            fn contains(&self, rhs: &$second) -> BooleanArray {
                assert_eq!(self.len(), rhs.len());

                let mut output_array = BooleanBuilder::with_capacity(self.len());

                self.iter_geo()
                    .zip(rhs.iter_geo())
                    .for_each(|(first, second)| match (first, second) {
                        (Some(first), Some(second)) => {
                            output_array.append_value(first.contains(&second))
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
impl<'a> Contains<Point<'a>> for PointArray {
    fn contains(&self, rhs: &Point<'a>) -> BooleanArray {
        let mut output_array = BooleanBuilder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_point| {
            let output = maybe_point.map(|point| point.contains(&rhs.to_geo()));
            output_array.append_option(output)
        });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl_geoarrow_scalar {
    ($first:ty, $second:ty) => {
        impl<'a, O: OffsetSizeTrait> Contains<$second> for $first {
            fn contains(&self, rhs: &$second) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());
                let rhs_geo = rhs.to_geo();

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| geom.contains(&rhs_geo));
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
        impl<'a> Contains<$second> for $first {
            fn contains(&self, rhs: &$second) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| geom.contains(rhs));
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
        impl<'a, O: OffsetSizeTrait> Contains<$second> for $first {
            fn contains(&self, rhs: &$second) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| geom.contains(rhs));
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
