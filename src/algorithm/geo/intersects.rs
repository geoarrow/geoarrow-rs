use crate::array::*;
use crate::scalar::*;
use crate::trait_::GeometryScalarTrait;
use crate::GeometryArrayTrait;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{BooleanArray, OffsetSizeTrait};
use geo::Intersects as _Intersects;

/// Checks if the geometry Self intersects the geometry Rhs.
/// More formally, either boundary or interior of Self has
/// non-empty (set-theoretic) intersection with the boundary
/// or interior of Rhs. In other words, the [DE-9IM]
/// intersection matrix for (Self, Rhs) is _not_ `FF*FF****`.
///
/// This predicate is symmetric: `a.intersects(b)` iff
/// `b.intersects(a)`.
///
/// [DE-9IM]: https://en.wikipedia.org/wiki/DE-9IM
///
/// # Examples
///
/// ```
/// use geo::Intersects;
/// use geo::line_string;
///
/// let line_string_a = line_string![
///     (x: 3., y: 2.),
///     (x: 7., y: 6.),
/// ];
///
/// let line_string_b = line_string![
///     (x: 3., y: 4.),
///     (x: 8., y: 4.),
/// ];
///
/// let line_string_c = line_string![
///     (x: 9., y: 2.),
///     (x: 11., y: 5.),
/// ];
///
/// assert!(line_string_a.intersects(&line_string_b));
/// assert!(!line_string_a.intersects(&line_string_c));
/// ```
pub trait Intersects<Rhs = Self> {
    fn intersects(&self, rhs: &Rhs) -> BooleanArray;
}

// ┌────────────────────────────────┐
// │ Implementations for RHS arrays │
// └────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl Intersects for PointArray {
    fn intersects(&self, rhs: &Self) -> BooleanArray {
        assert_eq!(self.len(), rhs.len());

        let mut output_array = BooleanBuilder::with_capacity(self.len());

        self.iter_geo()
            .zip(rhs.iter_geo())
            .for_each(|(first, second)| match (first, second) {
                (Some(first), Some(second)) => output_array.append_value(first.intersects(&second)),
                _ => output_array.append_null(),
            });

        output_array.finish()
    }
}

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a, O: OffsetSizeTrait> Intersects<$second> for $first {
            fn intersects(&self, rhs: &$second) -> BooleanArray {
                assert_eq!(self.len(), rhs.len());

                let mut output_array = BooleanBuilder::with_capacity(self.len());

                self.iter_geo()
                    .zip(rhs.iter_geo())
                    .for_each(|(first, second)| match (first, second) {
                        (Some(first), Some(second)) => {
                            output_array.append_value(first.intersects(&second))
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

// Implementations on WKBArray
iter_geo_impl!(WKBArray<O>, PointArray);
iter_geo_impl!(WKBArray<O>, LineStringArray<O>);
iter_geo_impl!(WKBArray<O>, PolygonArray<O>);
iter_geo_impl!(WKBArray<O>, MultiPointArray<O>);
iter_geo_impl!(WKBArray<O>, MultiLineStringArray<O>);
iter_geo_impl!(WKBArray<O>, MultiPolygonArray<O>);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

// Note: this implementation is outside the macro because it is not generic over O
impl<'a> Intersects<Point<'a>> for PointArray {
    fn intersects(&self, rhs: &Point<'a>) -> BooleanArray {
        let mut output_array = BooleanBuilder::with_capacity(self.len());

        self.iter_geo().for_each(|maybe_point| {
            let output = maybe_point.map(|point| point.intersects(&rhs.to_geo()));
            output_array.append_option(output)
        });

        output_array.finish()
    }
}

/// Implementation that iterates over geo objects
macro_rules! iter_geo_impl_scalar {
    ($first:ty, $second:ty) => {
        impl<'a, O: OffsetSizeTrait> Intersects<$second> for $first {
            fn intersects(&self, rhs: &$second) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());
                let rhs_geo = rhs.to_geo();

                self.iter_geo().for_each(|maybe_geom| {
                    let output = maybe_geom.map(|geom| geom.intersects(&rhs_geo));
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
iter_geo_impl_scalar!(LineStringArray<O>, MultiPoint<'a, O>);
iter_geo_impl_scalar!(LineStringArray<O>, MultiLineString<'a, O>);
iter_geo_impl_scalar!(LineStringArray<O>, MultiPolygon<'a, O>);

// Implementations on PolygonArray
iter_geo_impl_scalar!(PolygonArray<O>, Point<'a>);
iter_geo_impl_scalar!(PolygonArray<O>, LineString<'a, O>);
iter_geo_impl_scalar!(PolygonArray<O>, Polygon<'a, O>);
iter_geo_impl_scalar!(PolygonArray<O>, MultiPoint<'a, O>);
iter_geo_impl_scalar!(PolygonArray<O>, MultiLineString<'a, O>);
iter_geo_impl_scalar!(PolygonArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiPointArray
iter_geo_impl_scalar!(MultiPointArray<O>, Point<'a>);
iter_geo_impl_scalar!(MultiPointArray<O>, LineString<'a, O>);
iter_geo_impl_scalar!(MultiPointArray<O>, Polygon<'a, O>);
iter_geo_impl_scalar!(MultiPointArray<O>, MultiPoint<'a, O>);
iter_geo_impl_scalar!(MultiPointArray<O>, MultiLineString<'a, O>);
iter_geo_impl_scalar!(MultiPointArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiLineStringArray
iter_geo_impl_scalar!(MultiLineStringArray<O>, Point<'a>);
iter_geo_impl_scalar!(MultiLineStringArray<O>, LineString<'a, O>);
iter_geo_impl_scalar!(MultiLineStringArray<O>, Polygon<'a, O>);
iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiPoint<'a, O>);
iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiLineString<'a, O>);
iter_geo_impl_scalar!(MultiLineStringArray<O>, MultiPolygon<'a, O>);

// Implementations on MultiPolygonArray
iter_geo_impl_scalar!(MultiPolygonArray<O>, Point<'a>);
iter_geo_impl_scalar!(MultiPolygonArray<O>, LineString<'a, O>);
iter_geo_impl_scalar!(MultiPolygonArray<O>, Polygon<'a, O>);
iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiPoint<'a, O>);
iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiLineString<'a, O>);
iter_geo_impl_scalar!(MultiPolygonArray<O>, MultiPolygon<'a, O>);

// Implementations on WKBArray
iter_geo_impl_scalar!(WKBArray<O>, Point<'a>);
iter_geo_impl_scalar!(WKBArray<O>, LineString<'a, O>);
iter_geo_impl_scalar!(WKBArray<O>, Polygon<'a, O>);
iter_geo_impl_scalar!(WKBArray<O>, MultiPoint<'a, O>);
iter_geo_impl_scalar!(WKBArray<O>, MultiLineString<'a, O>);
iter_geo_impl_scalar!(WKBArray<O>, MultiPolygon<'a, O>);

#[cfg(test)]
mod tests {
    use arrow_array::Float64Array;
    use geo::{point, polygon};
    use crate::algorithm::geo::Intersects;
    use crate::array::{CoordBuffer, InterleavedCoordBuffer, MultiPolygonArray, PointArray, PolygonArray, WKBArray};
    use crate::scalar::Point;

    #[test]
    fn wkbarray_intersects_arrays() {
        let polygon_array: PolygonArray<i32> = vec![
            polygon![
                (x: 0., y: 0.),
                (x: 0., y: 2.),
                (x: 2., y: 2.),
                (x: 2., y: 0.),
            ],
            polygon![
                (x: 2., y: 2.),
                (x: 2., y: 4.),
                (x: 4., y: 4.),
                (x: 4., y: 2.),
            ],
        ].into();
        let wkb_array: WKBArray<i32> = (&polygon_array).into();

        let point_array: PointArray = vec![point!(x: 1.0, y: 1.0), point!(x: 1.1, y: 1.1)].into();

        let result = wkb_array.intersects(&point_array);
        assert_eq!(result.len(), 2);
        assert!(result.value(0));
        assert!(!result.value(1));
    }

    #[test]
    fn wkbarray_intersects_scalars() {
        let polygon_array: PolygonArray<i32> = vec![
            polygon![
                (x: 0., y: 0.),
                (x: 0., y: 2.),
                (x: 2., y: 2.),
                (x: 2., y: 0.),
            ],
            polygon![
                (x: 2., y: 2.),
                (x: 2., y: 4.),
                (x: 4., y: 4.),
                (x: 4., y: 2.),
            ],
        ].into();
        let wkb_array: WKBArray<i32> = (&polygon_array).into();

        let (_, buf, _) =
            Float64Array::from(vec![1.0, 1.0]).into_parts();
        let buf = CoordBuffer::Interleaved(InterleavedCoordBuffer::new(buf));
        let point = Point::new_owned(buf, 0);

        let result = wkb_array.intersects(&point);
        assert_eq!(result.len(), 2);
        assert!(result.value(0));
        assert!(!result.value(1));
    }
}
