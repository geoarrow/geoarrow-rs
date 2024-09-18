use crate::algorithm::native::{Binary, Unary};
use crate::array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::GeoArrowError;
use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use crate::io::geo::{
    geometry_collection_to_geo, geometry_to_geo, line_string_to_geo, multi_line_string_to_geo,
    multi_point_to_geo, multi_polygon_to_geo, point_to_geo, polygon_to_geo,
};
use crate::trait_::{NativeArrayAccessor, NativeScalar};
use crate::NativeArray;
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
impl Contains for PointArray<2> {
    fn contains(&self, rhs: &Self) -> BooleanArray {
        self.try_binary_boolean(rhs, |left, right| {
            Ok(left.to_geo().contains(&right.to_geo()))
        })
        .unwrap()
    }
}

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a, O: OffsetSizeTrait> Contains<$second> for $first {
            fn contains(&self, rhs: &$second) -> BooleanArray {
                self.try_binary_boolean(rhs, |left, right| {
                    Ok(left.to_geo().contains(&right.to_geo()))
                })
                .unwrap()
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

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

pub trait ContainsPoint<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: PointTrait<T = f64>> ContainsPoint<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = point_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_point {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: PointTrait<T = f64>> ContainsPoint<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = point_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_point!(LineStringArray<O, 2>);
impl_contains_point!(PolygonArray<O, 2>);
impl_contains_point!(MultiPointArray<O, 2>);
impl_contains_point!(MultiLineStringArray<O, 2>);
impl_contains_point!(MultiPolygonArray<O, 2>);
impl_contains_point!(MixedGeometryArray<O, 2>);
impl_contains_point!(GeometryCollectionArray<O, 2>);

impl<G: PointTrait<T = f64>> ContainsPoint<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsPoint::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => ContainsPoint::contains(self.as_line_string::<2>(), rhs),
            LargeLineString(_, XY) => {
                ContainsPoint::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsPoint::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => ContainsPoint::contains(self.as_large_polygon::<2>(), rhs),
            MultiPoint(_, XY) => ContainsPoint::contains(self.as_multi_point::<2>(), rhs),
            LargeMultiPoint(_, XY) => {
                ContainsPoint::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsPoint::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsPoint::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => ContainsPoint::contains(self.as_multi_polygon::<2>(), rhs),
            LargeMultiPolygon(_, XY) => {
                ContainsPoint::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsPoint::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => ContainsPoint::contains(self.as_large_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                ContainsPoint::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsPoint::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsLineString<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: LineStringTrait<T = f64>> ContainsLineString<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = line_string_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_line_string {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> ContainsLineString<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());
                let rhs = line_string_to_geo(rhs);

                self.iter_geo().for_each(|maybe_point| {
                    let output = maybe_point.map(|point| point.contains(&rhs));
                    output_array.append_option(output)
                });

                output_array.finish()
            }
        }
    };
}

impl_contains_line_string!(LineStringArray<O, 2>);
impl_contains_line_string!(PolygonArray<O, 2>);
impl_contains_line_string!(MultiPointArray<O, 2>);
impl_contains_line_string!(MultiLineStringArray<O, 2>);
impl_contains_line_string!(MultiPolygonArray<O, 2>);
impl_contains_line_string!(MixedGeometryArray<O, 2>);
impl_contains_line_string!(GeometryCollectionArray<O, 2>);

impl<G: LineStringTrait<T = f64>> ContainsLineString<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsLineString::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => ContainsLineString::contains(self.as_line_string::<2>(), rhs),
            LargeLineString(_, XY) => {
                ContainsLineString::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsLineString::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => ContainsLineString::contains(self.as_large_polygon::<2>(), rhs),
            MultiPoint(_, XY) => ContainsLineString::contains(self.as_multi_point::<2>(), rhs),
            LargeMultiPoint(_, XY) => {
                ContainsLineString::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsLineString::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsLineString::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => ContainsLineString::contains(self.as_multi_polygon::<2>(), rhs),
            LargeMultiPolygon(_, XY) => {
                ContainsLineString::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsLineString::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => ContainsLineString::contains(self.as_large_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                ContainsLineString::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsLineString::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsPolygon<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: PolygonTrait<T = f64>> ContainsPolygon<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = polygon_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_polygon {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> ContainsPolygon<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = polygon_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_polygon!(LineStringArray<O, 2>);
impl_contains_polygon!(PolygonArray<O, 2>);
impl_contains_polygon!(MultiPointArray<O, 2>);
impl_contains_polygon!(MultiLineStringArray<O, 2>);
impl_contains_polygon!(MultiPolygonArray<O, 2>);
impl_contains_polygon!(MixedGeometryArray<O, 2>);
impl_contains_polygon!(GeometryCollectionArray<O, 2>);

impl<G: PolygonTrait<T = f64>> ContainsPolygon<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsPolygon::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => ContainsPolygon::contains(self.as_line_string::<2>(), rhs),
            LargeLineString(_, XY) => {
                ContainsPolygon::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsPolygon::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => ContainsPolygon::contains(self.as_large_polygon::<2>(), rhs),
            MultiPoint(_, XY) => ContainsPolygon::contains(self.as_multi_point::<2>(), rhs),
            LargeMultiPoint(_, XY) => {
                ContainsPolygon::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsPolygon::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsPolygon::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => ContainsPolygon::contains(self.as_multi_polygon::<2>(), rhs),
            LargeMultiPolygon(_, XY) => {
                ContainsPolygon::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsPolygon::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => ContainsPolygon::contains(self.as_large_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                ContainsPolygon::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsPolygon::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsMultiPoint<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: MultiPointTrait<T = f64>> ContainsMultiPoint<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = multi_point_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_multi_point {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> ContainsMultiPoint<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = multi_point_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_multi_point!(LineStringArray<O, 2>);
impl_contains_multi_point!(PolygonArray<O, 2>);
impl_contains_multi_point!(MultiPointArray<O, 2>);
impl_contains_multi_point!(MultiLineStringArray<O, 2>);
impl_contains_multi_point!(MultiPolygonArray<O, 2>);
impl_contains_multi_point!(MixedGeometryArray<O, 2>);
impl_contains_multi_point!(GeometryCollectionArray<O, 2>);

impl<G: MultiPointTrait<T = f64>> ContainsMultiPoint<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsMultiPoint::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => ContainsMultiPoint::contains(self.as_line_string::<2>(), rhs),
            LargeLineString(_, XY) => {
                ContainsMultiPoint::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsMultiPoint::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => ContainsMultiPoint::contains(self.as_large_polygon::<2>(), rhs),
            MultiPoint(_, XY) => ContainsMultiPoint::contains(self.as_multi_point::<2>(), rhs),
            LargeMultiPoint(_, XY) => {
                ContainsMultiPoint::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsMultiPoint::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsMultiPoint::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => ContainsMultiPoint::contains(self.as_multi_polygon::<2>(), rhs),
            LargeMultiPolygon(_, XY) => {
                ContainsMultiPoint::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsMultiPoint::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => ContainsMultiPoint::contains(self.as_large_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                ContainsMultiPoint::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsMultiPoint::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsMultiLineString<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: MultiLineStringTrait<T = f64>> ContainsMultiLineString<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = multi_line_string_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_multi_line_string {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> ContainsMultiLineString<G>
            for $array
        {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = multi_line_string_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_multi_line_string!(LineStringArray<O, 2>);
impl_contains_multi_line_string!(PolygonArray<O, 2>);
impl_contains_multi_line_string!(MultiPointArray<O, 2>);
impl_contains_multi_line_string!(MultiLineStringArray<O, 2>);
impl_contains_multi_line_string!(MultiPolygonArray<O, 2>);
impl_contains_multi_line_string!(MixedGeometryArray<O, 2>);
impl_contains_multi_line_string!(GeometryCollectionArray<O, 2>);

impl<G: MultiLineStringTrait<T = f64>> ContainsMultiLineString<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsMultiLineString::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => ContainsMultiLineString::contains(self.as_line_string::<2>(), rhs),
            LargeLineString(_, XY) => {
                ContainsMultiLineString::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsMultiLineString::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => {
                ContainsMultiLineString::contains(self.as_large_polygon::<2>(), rhs)
            }
            MultiPoint(_, XY) => ContainsMultiLineString::contains(self.as_multi_point::<2>(), rhs),
            LargeMultiPoint(_, XY) => {
                ContainsMultiLineString::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsMultiLineString::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsMultiLineString::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => {
                ContainsMultiLineString::contains(self.as_multi_polygon::<2>(), rhs)
            }
            LargeMultiPolygon(_, XY) => {
                ContainsMultiLineString::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsMultiLineString::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => ContainsMultiLineString::contains(self.as_large_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                ContainsMultiLineString::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsMultiLineString::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsMultiPolygon<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: MultiPolygonTrait<T = f64>> ContainsMultiPolygon<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = multi_polygon_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_multi_polygon {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> ContainsMultiPolygon<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = multi_polygon_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_multi_polygon!(LineStringArray<O, 2>);
impl_contains_multi_polygon!(PolygonArray<O, 2>);
impl_contains_multi_polygon!(MultiPointArray<O, 2>);
impl_contains_multi_polygon!(MultiLineStringArray<O, 2>);
impl_contains_multi_polygon!(MultiPolygonArray<O, 2>);
impl_contains_multi_polygon!(MixedGeometryArray<O, 2>);
impl_contains_multi_polygon!(GeometryCollectionArray<O, 2>);

impl<G: MultiPolygonTrait<T = f64>> ContainsMultiPolygon<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsMultiPolygon::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => ContainsMultiPolygon::contains(self.as_line_string::<2>(), rhs),
            LargeLineString(_, XY) => {
                ContainsMultiPolygon::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsMultiPolygon::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => {
                ContainsMultiPolygon::contains(self.as_large_polygon::<2>(), rhs)
            }
            MultiPoint(_, XY) => ContainsMultiPolygon::contains(self.as_multi_point::<2>(), rhs),
            LargeMultiPoint(_, XY) => {
                ContainsMultiPolygon::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsMultiPolygon::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsMultiPolygon::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => {
                ContainsMultiPolygon::contains(self.as_multi_polygon::<2>(), rhs)
            }
            LargeMultiPolygon(_, XY) => {
                ContainsMultiPolygon::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsMultiPolygon::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => ContainsMultiPolygon::contains(self.as_large_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                ContainsMultiPolygon::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsMultiPolygon::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsGeometry<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = geometry_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_geometry {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> ContainsGeometry<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = geometry_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_geometry!(LineStringArray<O, 2>);
impl_contains_geometry!(PolygonArray<O, 2>);
// impl_contains_geometry!(MultiPointArray<O, 2>); // Not implemented in geo
impl_contains_geometry!(MultiLineStringArray<O, 2>);
// impl_contains_geometry!(MultiPolygonArray<O, 2>); // Not implemented in geo
impl_contains_geometry!(MixedGeometryArray<O, 2>);
impl_contains_geometry!(GeometryCollectionArray<O, 2>);

impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsGeometry::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => ContainsGeometry::contains(self.as_line_string::<2>(), rhs),
            LargeLineString(_, XY) => {
                ContainsGeometry::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsGeometry::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => ContainsGeometry::contains(self.as_large_polygon::<2>(), rhs),
            MultiPoint(_, XY) => todo!(), // ContainsGeometry::contains(self.as_multi_point::<2>(), rhs),
            LargeMultiPoint(_, XY) => {
                todo!()
                // ContainsGeometry::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsGeometry::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsGeometry::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => todo!(), // ContainsGeometry::contains(self.as_multi_polygon::<2>(), rhs),
            LargeMultiPolygon(_, XY) => {
                todo!()
                // ContainsGeometry::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsGeometry::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => ContainsGeometry::contains(self.as_large_mixed::<2>(), rhs),
            GeometryCollection(_, XY) => {
                ContainsGeometry::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsGeometry::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsGeometryCollection<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: GeometryCollectionTrait<T = f64>> ContainsGeometryCollection<G> for PointArray<2> {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = geometry_collection_to_geo(rhs);
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_geometry_collection {
    ($array:ty) => {
        impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> ContainsGeometryCollection<G>
            for $array
        {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = geometry_collection_to_geo(rhs);
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_geometry_collection!(LineStringArray<O, 2>);
impl_contains_geometry_collection!(PolygonArray<O, 2>);
impl_contains_geometry_collection!(MultiPointArray<O, 2>);
impl_contains_geometry_collection!(MultiLineStringArray<O, 2>);
impl_contains_geometry_collection!(MultiPolygonArray<O, 2>);
impl_contains_geometry_collection!(MixedGeometryArray<O, 2>);
impl_contains_geometry_collection!(GeometryCollectionArray<O, 2>);

impl<G: GeometryCollectionTrait<T = f64>> ContainsGeometryCollection<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => ContainsGeometryCollection::contains(self.as_point::<2>(), rhs),
            LineString(_, XY) => {
                ContainsGeometryCollection::contains(self.as_line_string::<2>(), rhs)
            }
            LargeLineString(_, XY) => {
                ContainsGeometryCollection::contains(self.as_large_line_string::<2>(), rhs)
            }
            Polygon(_, XY) => ContainsGeometryCollection::contains(self.as_polygon::<2>(), rhs),
            LargePolygon(_, XY) => {
                ContainsGeometryCollection::contains(self.as_large_polygon::<2>(), rhs)
            }
            MultiPoint(_, XY) => {
                ContainsGeometryCollection::contains(self.as_multi_point::<2>(), rhs)
            }
            LargeMultiPoint(_, XY) => {
                ContainsGeometryCollection::contains(self.as_large_multi_point::<2>(), rhs)
            }
            MultiLineString(_, XY) => {
                ContainsGeometryCollection::contains(self.as_multi_line_string::<2>(), rhs)
            }
            LargeMultiLineString(_, XY) => {
                ContainsGeometryCollection::contains(self.as_large_multi_line_string::<2>(), rhs)
            }
            MultiPolygon(_, XY) => {
                ContainsGeometryCollection::contains(self.as_multi_polygon::<2>(), rhs)
            }
            LargeMultiPolygon(_, XY) => {
                ContainsGeometryCollection::contains(self.as_large_multi_polygon::<2>(), rhs)
            }
            Mixed(_, XY) => ContainsGeometryCollection::contains(self.as_mixed::<2>(), rhs),
            LargeMixed(_, XY) => {
                ContainsGeometryCollection::contains(self.as_large_mixed::<2>(), rhs)
            }
            GeometryCollection(_, XY) => {
                ContainsGeometryCollection::contains(self.as_geometry_collection::<2>(), rhs)
            }
            LargeGeometryCollection(_, XY) => {
                ContainsGeometryCollection::contains(self.as_large_geometry_collection::<2>(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
