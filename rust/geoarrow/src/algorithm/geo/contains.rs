use crate::algorithm::native::{Binary, Unary};
use crate::array::*;
use crate::datatypes::NativeType;
use crate::error::GeoArrowError;
use crate::io::geo::geometry_to_geo;
use crate::trait_::NativeScalar;
use crate::NativeArray;
use arrow_array::BooleanArray;
use geo::Contains as _Contains;
use geo_traits::GeometryTrait;

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

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a> Contains<$second> for $first {
            fn contains(&self, rhs: &$second) -> BooleanArray {
                self.try_binary_boolean(rhs, |left, right| {
                    Ok(left.to_geo_geometry().contains(&right.to_geo_geometry()))
                })
                .unwrap()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(PointArray, PointArray);
iter_geo_impl!(PointArray, LineStringArray);
iter_geo_impl!(PointArray, PolygonArray);
iter_geo_impl!(PointArray, MultiPointArray);
iter_geo_impl!(PointArray, MultiLineStringArray);
iter_geo_impl!(PointArray, MultiPolygonArray);
iter_geo_impl!(PointArray, RectArray);
iter_geo_impl!(PointArray, GeometryArray);

// Implementations on LineStringArray
iter_geo_impl!(LineStringArray, PointArray);
iter_geo_impl!(LineStringArray, LineStringArray);
iter_geo_impl!(LineStringArray, PolygonArray);
iter_geo_impl!(LineStringArray, MultiPointArray);
iter_geo_impl!(LineStringArray, MultiLineStringArray);
iter_geo_impl!(LineStringArray, MultiPolygonArray);
iter_geo_impl!(LineStringArray, RectArray);
iter_geo_impl!(LineStringArray, GeometryArray);

// Implementations on PolygonArray
iter_geo_impl!(PolygonArray, PointArray);
iter_geo_impl!(PolygonArray, LineStringArray);
iter_geo_impl!(PolygonArray, PolygonArray);
iter_geo_impl!(PolygonArray, MultiPointArray);
iter_geo_impl!(PolygonArray, MultiLineStringArray);
iter_geo_impl!(PolygonArray, MultiPolygonArray);
iter_geo_impl!(PolygonArray, RectArray);
iter_geo_impl!(PolygonArray, GeometryArray);

// Implementations on MultiPointArray
iter_geo_impl!(MultiPointArray, PointArray);
iter_geo_impl!(MultiPointArray, LineStringArray);
iter_geo_impl!(MultiPointArray, PolygonArray);
iter_geo_impl!(MultiPointArray, MultiPointArray);
iter_geo_impl!(MultiPointArray, MultiLineStringArray);
iter_geo_impl!(MultiPointArray, MultiPolygonArray);
iter_geo_impl!(MultiPointArray, RectArray);
iter_geo_impl!(MultiPointArray, GeometryArray);

// Implementations on MultiLineStringArray
iter_geo_impl!(MultiLineStringArray, PointArray);
iter_geo_impl!(MultiLineStringArray, LineStringArray);
iter_geo_impl!(MultiLineStringArray, PolygonArray);
iter_geo_impl!(MultiLineStringArray, MultiPointArray);
iter_geo_impl!(MultiLineStringArray, MultiLineStringArray);
iter_geo_impl!(MultiLineStringArray, MultiPolygonArray);
iter_geo_impl!(MultiLineStringArray, RectArray);
iter_geo_impl!(MultiLineStringArray, GeometryArray);

// Implementations on MultiPolygonArray
iter_geo_impl!(MultiPolygonArray, PointArray);
iter_geo_impl!(MultiPolygonArray, LineStringArray);
iter_geo_impl!(MultiPolygonArray, PolygonArray);
iter_geo_impl!(MultiPolygonArray, MultiPointArray);
iter_geo_impl!(MultiPolygonArray, MultiLineStringArray);
iter_geo_impl!(MultiPolygonArray, MultiPolygonArray);
iter_geo_impl!(MultiPolygonArray, RectArray);
iter_geo_impl!(MultiPolygonArray, GeometryArray);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

pub trait ContainsGeometry<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = geometry_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_point {
    ($array:ty) => {
        impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = geometry_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| {
                    Ok(geom.to_geo_geometry().contains(&rhs))
                })
                .unwrap()
            }
        }
    };
}

impl_contains_point!(LineStringArray);
impl_contains_point!(PolygonArray);
impl_contains_point!(MultiPointArray);
impl_contains_point!(MultiLineStringArray);
impl_contains_point!(MultiPolygonArray);
impl_contains_point!(MixedGeometryArray);
impl_contains_point!(GeometryCollectionArray);
impl_contains_point!(GeometryArray);
impl_contains_point!(RectArray);

impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use NativeType::*;

        match self.data_type() {
            Point(_) => ContainsGeometry::contains(self.as_point(), rhs),
            LineString(_) => ContainsGeometry::contains(self.as_line_string(), rhs),
            Polygon(_) => ContainsGeometry::contains(self.as_polygon(), rhs),
            MultiPoint(_) => ContainsGeometry::contains(self.as_multi_point(), rhs),
            MultiLineString(_) => ContainsGeometry::contains(self.as_multi_line_string(), rhs),
            MultiPolygon(_) => ContainsGeometry::contains(self.as_multi_polygon(), rhs),
            GeometryCollection(_) => ContainsGeometry::contains(self.as_geometry_collection(), rhs),
            Rect(_) => ContainsGeometry::contains(self.as_rect(), rhs),
            Geometry(_) => ContainsGeometry::contains(self.as_geometry(), rhs),
        }
    }
}
