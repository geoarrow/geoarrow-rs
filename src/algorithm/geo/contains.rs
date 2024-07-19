use crate::algorithm::native::{Binary, Unary};
use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::GeoArrowError;
use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use crate::io::geo::{
    geometry_collection_to_geo, geometry_to_geo, line_string_to_geo, multi_line_string_to_geo,
    multi_point_to_geo, multi_polygon_to_geo, point_to_geo, polygon_to_geo,
};
use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};
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

impl<G: PointTrait<T = f64>> ContainsPoint<G> for &dyn GeometryArrayTrait {
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsPoint::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => ContainsPoint::contains(self.as_line_string(), rhs),
            GeoDataType::LargeLineString(_) => {
                ContainsPoint::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsPoint::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => ContainsPoint::contains(self.as_large_polygon(), rhs),
            GeoDataType::MultiPoint(_) => ContainsPoint::contains(self.as_multi_point(), rhs),
            GeoDataType::LargeMultiPoint(_) => {
                ContainsPoint::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsPoint::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsPoint::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => ContainsPoint::contains(self.as_multi_polygon(), rhs),
            GeoDataType::LargeMultiPolygon(_) => {
                ContainsPoint::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsPoint::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => ContainsPoint::contains(self.as_large_mixed(), rhs),
            GeoDataType::GeometryCollection(_) => {
                ContainsPoint::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsPoint::contains(self.as_large_geometry_collection(), rhs)
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

impl<G: LineStringTrait<T = f64>> ContainsLineString<G> for &dyn GeometryArrayTrait {
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsLineString::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => ContainsLineString::contains(self.as_line_string(), rhs),
            GeoDataType::LargeLineString(_) => {
                ContainsLineString::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsLineString::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => {
                ContainsLineString::contains(self.as_large_polygon(), rhs)
            }
            GeoDataType::MultiPoint(_) => ContainsLineString::contains(self.as_multi_point(), rhs),
            GeoDataType::LargeMultiPoint(_) => {
                ContainsLineString::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsLineString::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsLineString::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => {
                ContainsLineString::contains(self.as_multi_polygon(), rhs)
            }
            GeoDataType::LargeMultiPolygon(_) => {
                ContainsLineString::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsLineString::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => ContainsLineString::contains(self.as_large_mixed(), rhs),
            GeoDataType::GeometryCollection(_) => {
                ContainsLineString::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsLineString::contains(self.as_large_geometry_collection(), rhs)
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

impl<G: PolygonTrait<T = f64>> ContainsPolygon<G> for &dyn GeometryArrayTrait {
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsPolygon::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => ContainsPolygon::contains(self.as_line_string(), rhs),
            GeoDataType::LargeLineString(_) => {
                ContainsPolygon::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsPolygon::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => ContainsPolygon::contains(self.as_large_polygon(), rhs),
            GeoDataType::MultiPoint(_) => ContainsPolygon::contains(self.as_multi_point(), rhs),
            GeoDataType::LargeMultiPoint(_) => {
                ContainsPolygon::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsPolygon::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsPolygon::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => ContainsPolygon::contains(self.as_multi_polygon(), rhs),
            GeoDataType::LargeMultiPolygon(_) => {
                ContainsPolygon::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsPolygon::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => ContainsPolygon::contains(self.as_large_mixed(), rhs),
            GeoDataType::GeometryCollection(_) => {
                ContainsPolygon::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsPolygon::contains(self.as_large_geometry_collection(), rhs)
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

impl<G: MultiPointTrait<T = f64>> ContainsMultiPoint<G> for &dyn GeometryArrayTrait {
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsMultiPoint::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => ContainsMultiPoint::contains(self.as_line_string(), rhs),
            GeoDataType::LargeLineString(_) => {
                ContainsMultiPoint::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsMultiPoint::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => {
                ContainsMultiPoint::contains(self.as_large_polygon(), rhs)
            }
            GeoDataType::MultiPoint(_) => ContainsMultiPoint::contains(self.as_multi_point(), rhs),
            GeoDataType::LargeMultiPoint(_) => {
                ContainsMultiPoint::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsMultiPoint::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsMultiPoint::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => {
                ContainsMultiPoint::contains(self.as_multi_polygon(), rhs)
            }
            GeoDataType::LargeMultiPolygon(_) => {
                ContainsMultiPoint::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsMultiPoint::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => ContainsMultiPoint::contains(self.as_large_mixed(), rhs),
            GeoDataType::GeometryCollection(_) => {
                ContainsMultiPoint::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsMultiPoint::contains(self.as_large_geometry_collection(), rhs)
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

impl<G: MultiLineStringTrait<T = f64>> ContainsMultiLineString<G> for &dyn GeometryArrayTrait {
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsMultiLineString::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => {
                ContainsMultiLineString::contains(self.as_line_string(), rhs)
            }
            GeoDataType::LargeLineString(_) => {
                ContainsMultiLineString::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsMultiLineString::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => {
                ContainsMultiLineString::contains(self.as_large_polygon(), rhs)
            }
            GeoDataType::MultiPoint(_) => {
                ContainsMultiLineString::contains(self.as_multi_point(), rhs)
            }
            GeoDataType::LargeMultiPoint(_) => {
                ContainsMultiLineString::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsMultiLineString::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsMultiLineString::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => {
                ContainsMultiLineString::contains(self.as_multi_polygon(), rhs)
            }
            GeoDataType::LargeMultiPolygon(_) => {
                ContainsMultiLineString::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsMultiLineString::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => {
                ContainsMultiLineString::contains(self.as_large_mixed(), rhs)
            }
            GeoDataType::GeometryCollection(_) => {
                ContainsMultiLineString::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsMultiLineString::contains(self.as_large_geometry_collection(), rhs)
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

impl<G: MultiPolygonTrait<T = f64>> ContainsMultiPolygon<G> for &dyn GeometryArrayTrait {
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsMultiPolygon::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => {
                ContainsMultiPolygon::contains(self.as_line_string(), rhs)
            }
            GeoDataType::LargeLineString(_) => {
                ContainsMultiPolygon::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsMultiPolygon::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => {
                ContainsMultiPolygon::contains(self.as_large_polygon(), rhs)
            }
            GeoDataType::MultiPoint(_) => {
                ContainsMultiPolygon::contains(self.as_multi_point(), rhs)
            }
            GeoDataType::LargeMultiPoint(_) => {
                ContainsMultiPolygon::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsMultiPolygon::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsMultiPolygon::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => {
                ContainsMultiPolygon::contains(self.as_multi_polygon(), rhs)
            }
            GeoDataType::LargeMultiPolygon(_) => {
                ContainsMultiPolygon::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsMultiPolygon::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => {
                ContainsMultiPolygon::contains(self.as_large_mixed(), rhs)
            }
            GeoDataType::GeometryCollection(_) => {
                ContainsMultiPolygon::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsMultiPolygon::contains(self.as_large_geometry_collection(), rhs)
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

impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for &dyn GeometryArrayTrait {
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsGeometry::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => ContainsGeometry::contains(self.as_line_string(), rhs),
            GeoDataType::LargeLineString(_) => {
                ContainsGeometry::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsGeometry::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => {
                ContainsGeometry::contains(self.as_large_polygon(), rhs)
            }
            GeoDataType::MultiPoint(_) => todo!(), // ContainsGeometry::contains(self.as_multi_point(), rhs),
            GeoDataType::LargeMultiPoint(_) => {
                todo!()
                // ContainsGeometry::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsGeometry::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsGeometry::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => todo!(), // ContainsGeometry::contains(self.as_multi_polygon(), rhs),
            GeoDataType::LargeMultiPolygon(_) => {
                todo!()
                // ContainsGeometry::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsGeometry::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => ContainsGeometry::contains(self.as_large_mixed(), rhs),
            GeoDataType::GeometryCollection(_) => {
                ContainsGeometry::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsGeometry::contains(self.as_large_geometry_collection(), rhs)
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

impl<G: GeometryCollectionTrait<T = f64>> ContainsGeometryCollection<G>
    for &dyn GeometryArrayTrait
{
    fn contains(&self, rhs: &G) -> BooleanArray {
        match self.data_type() {
            GeoDataType::Point(_) => ContainsGeometryCollection::contains(self.as_point(), rhs),
            GeoDataType::LineString(_) => {
                ContainsGeometryCollection::contains(self.as_line_string(), rhs)
            }
            GeoDataType::LargeLineString(_) => {
                ContainsGeometryCollection::contains(self.as_large_line_string(), rhs)
            }
            GeoDataType::Polygon(_) => ContainsGeometryCollection::contains(self.as_polygon(), rhs),
            GeoDataType::LargePolygon(_) => {
                ContainsGeometryCollection::contains(self.as_large_polygon(), rhs)
            }
            GeoDataType::MultiPoint(_) => {
                ContainsGeometryCollection::contains(self.as_multi_point(), rhs)
            }
            GeoDataType::LargeMultiPoint(_) => {
                ContainsGeometryCollection::contains(self.as_large_multi_point(), rhs)
            }
            GeoDataType::MultiLineString(_) => {
                ContainsGeometryCollection::contains(self.as_multi_line_string(), rhs)
            }
            GeoDataType::LargeMultiLineString(_) => {
                ContainsGeometryCollection::contains(self.as_large_multi_line_string(), rhs)
            }
            GeoDataType::MultiPolygon(_) => {
                ContainsGeometryCollection::contains(self.as_multi_polygon(), rhs)
            }
            GeoDataType::LargeMultiPolygon(_) => {
                ContainsGeometryCollection::contains(self.as_large_multi_polygon(), rhs)
            }
            GeoDataType::Mixed(_) => ContainsGeometryCollection::contains(self.as_mixed(), rhs),
            GeoDataType::LargeMixed(_) => {
                ContainsGeometryCollection::contains(self.as_large_mixed(), rhs)
            }
            GeoDataType::GeometryCollection(_) => {
                ContainsGeometryCollection::contains(self.as_geometry_collection(), rhs)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                ContainsGeometryCollection::contains(self.as_large_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
