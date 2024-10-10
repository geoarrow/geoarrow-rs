use crate::algorithm::native::binary::try_binary_boolean_native_geometry;
use crate::algorithm::native::Unary;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::GeometryTrait;
use crate::io::geo::geometry_to_geo;
use crate::trait_::NativeGeometryAccessor;
use crate::trait_::NativeScalar;
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
pub trait Within<'a, Other> {
    fn is_within(&'a self, b: &'a Other) -> Result<BooleanArray>;
}

macro_rules! iter_geo_impl {
    ($array_type:ty) => {
        impl<'a, R: NativeGeometryAccessor<'a, 2>> Within<'a, R> for $array_type {
            fn is_within(&'a self, rhs: &'a R) -> Result<BooleanArray> {
                try_binary_boolean_native_geometry(self, rhs, |l, r| {
                    Ok(l.to_geo().is_within(&r.to_geo()))
                })
            }
        }
    };
}

iter_geo_impl!(PointArray<2>);
iter_geo_impl!(LineStringArray<2>);
iter_geo_impl!(PolygonArray<2>);
iter_geo_impl!(MultiPointArray<2>);
iter_geo_impl!(MultiLineStringArray<2>);
iter_geo_impl!(MultiPolygonArray<2>);
iter_geo_impl!(MixedGeometryArray<2>);
iter_geo_impl!(GeometryCollectionArray<2>);
iter_geo_impl!(RectArray<2>);

impl<'a, R: NativeGeometryAccessor<'a, 2>> Within<'a, R> for &dyn NativeArray {
    fn is_within(&'a self, rhs: &'a R) -> Result<BooleanArray> {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => Within::is_within(self.as_point::<2>(), rhs),
            LineString(_, XY) => Within::is_within(self.as_line_string::<2>(), rhs),
            Polygon(_, XY) => Within::is_within(self.as_polygon::<2>(), rhs),
            MultiPoint(_, XY) => Within::is_within(self.as_multi_point::<2>(), rhs),
            MultiLineString(_, XY) => Within::is_within(self.as_multi_line_string::<2>(), rhs),
            MultiPolygon(_, XY) => Within::is_within(self.as_multi_polygon::<2>(), rhs),
            Mixed(_, XY) => Within::is_within(self.as_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => Within::is_within(self.as_geometry_collection::<2>(), rhs),
            Rect(XY) => Within::is_within(self.as_rect::<2>(), rhs),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait WithinScalar<'a, G: GeometryTrait> {
    fn is_within(&'a self, b: &'a G) -> Result<BooleanArray>;
}

macro_rules! scalar_impl {
    ($array_type:ty) => {
        impl<'a, G: GeometryTrait<T = f64>> WithinScalar<'a, G> for $array_type {
            fn is_within(&'a self, rhs: &'a G) -> Result<BooleanArray> {
                let right = geometry_to_geo(rhs);
                self.try_unary_boolean(|left| {
                    Ok::<_, GeoArrowError>(left.to_geo().is_within(&right))
                })
            }
        }
    };
}

scalar_impl!(PointArray<2>);
scalar_impl!(LineStringArray<2>);
scalar_impl!(PolygonArray<2>);
scalar_impl!(MultiPointArray<2>);
scalar_impl!(MultiLineStringArray<2>);
scalar_impl!(MultiPolygonArray<2>);
scalar_impl!(MixedGeometryArray<2>);
scalar_impl!(GeometryCollectionArray<2>);
scalar_impl!(RectArray<2>);

impl<'a, G: GeometryTrait<T = f64>> WithinScalar<'a, G> for &dyn NativeArray {
    fn is_within(&'a self, rhs: &'a G) -> Result<BooleanArray> {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => WithinScalar::is_within(self.as_point::<2>(), rhs),
            LineString(_, XY) => WithinScalar::is_within(self.as_line_string::<2>(), rhs),
            Polygon(_, XY) => WithinScalar::is_within(self.as_polygon::<2>(), rhs),
            MultiPoint(_, XY) => WithinScalar::is_within(self.as_multi_point::<2>(), rhs),
            MultiLineString(_, XY) => {
                WithinScalar::is_within(self.as_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => WithinScalar::is_within(self.as_multi_polygon::<2>(), rhs),
            Mixed(_, XY) => WithinScalar::is_within(self.as_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                WithinScalar::is_within(self.as_geometry_collection::<2>(), rhs)
            }
            Rect(XY) => WithinScalar::is_within(self.as_rect::<2>(), rhs),
            _ => Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
