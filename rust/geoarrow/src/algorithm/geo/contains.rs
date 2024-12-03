use crate::algorithm::native::{Binary, Unary};
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::GeoArrowError;
use crate::trait_::{ArrayAccessor, NativeScalar};
use crate::NativeArray;
use arrow_array::builder::BooleanBuilder;
use arrow_array::BooleanArray;
use geo::Contains as _Contains;
use geo_traits::to_geo::*;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};

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
        self.try_binary_boolean(rhs, |left, right| {
            Ok(left.to_geo().contains(&right.to_geo()))
        })
        .unwrap()
    }
}

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a> Contains<$second> for $first {
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

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

pub trait ContainsPoint<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: PointTrait<T = f64>> ContainsPoint<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_point();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_point {
    ($array:ty) => {
        impl<G: PointTrait<T = f64>> ContainsPoint<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = rhs.to_point();
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
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

impl<G: PointTrait<T = f64>> ContainsPoint<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsPoint::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsPoint::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsPoint::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => ContainsPoint::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => ContainsPoint::contains(self.as_multi_line_string(), rhs),
            MultiPolygon(_, XY) => ContainsPoint::contains(self.as_multi_polygon(), rhs),
            Mixed(_, XY) => ContainsPoint::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsPoint::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsLineString<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: LineStringTrait<T = f64>> ContainsLineString<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_line_string();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_line_string {
    ($array:ty) => {
        impl<G: LineStringTrait<T = f64>> ContainsLineString<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let mut output_array = BooleanBuilder::with_capacity(self.len());
                let rhs = rhs.to_line_string();

                self.iter_geo().for_each(|maybe_point| {
                    let output = maybe_point.map(|point| point.contains(&rhs));
                    output_array.append_option(output)
                });

                output_array.finish()
            }
        }
    };
}

impl_contains_line_string!(LineStringArray);
impl_contains_line_string!(PolygonArray);
impl_contains_line_string!(MultiPointArray);
impl_contains_line_string!(MultiLineStringArray);
impl_contains_line_string!(MultiPolygonArray);
impl_contains_line_string!(MixedGeometryArray);
impl_contains_line_string!(GeometryCollectionArray);

impl<G: LineStringTrait<T = f64>> ContainsLineString<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsLineString::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsLineString::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsLineString::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => ContainsLineString::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => {
                ContainsLineString::contains(self.as_multi_line_string(), rhs)
            }
            MultiPolygon(_, XY) => ContainsLineString::contains(self.as_multi_polygon(), rhs),
            Mixed(_, XY) => ContainsLineString::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsLineString::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsPolygon<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: PolygonTrait<T = f64>> ContainsPolygon<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_polygon();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_polygon {
    ($array:ty) => {
        impl<G: PolygonTrait<T = f64>> ContainsPolygon<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = rhs.to_polygon();
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_polygon!(LineStringArray);
impl_contains_polygon!(PolygonArray);
impl_contains_polygon!(MultiPointArray);
impl_contains_polygon!(MultiLineStringArray);
impl_contains_polygon!(MultiPolygonArray);
impl_contains_polygon!(MixedGeometryArray);
impl_contains_polygon!(GeometryCollectionArray);

impl<G: PolygonTrait<T = f64>> ContainsPolygon<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsPolygon::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsPolygon::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsPolygon::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => ContainsPolygon::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => ContainsPolygon::contains(self.as_multi_line_string(), rhs),
            MultiPolygon(_, XY) => ContainsPolygon::contains(self.as_multi_polygon(), rhs),
            Mixed(_, XY) => ContainsPolygon::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsPolygon::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsMultiPoint<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: MultiPointTrait<T = f64>> ContainsMultiPoint<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_multi_point();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_multi_point {
    ($array:ty) => {
        impl<G: MultiPointTrait<T = f64>> ContainsMultiPoint<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = rhs.to_multi_point();
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_multi_point!(LineStringArray);
impl_contains_multi_point!(PolygonArray);
impl_contains_multi_point!(MultiPointArray);
impl_contains_multi_point!(MultiLineStringArray);
impl_contains_multi_point!(MultiPolygonArray);
impl_contains_multi_point!(MixedGeometryArray);
impl_contains_multi_point!(GeometryCollectionArray);

impl<G: MultiPointTrait<T = f64>> ContainsMultiPoint<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsMultiPoint::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsMultiPoint::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsMultiPoint::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => ContainsMultiPoint::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => {
                ContainsMultiPoint::contains(self.as_multi_line_string(), rhs)
            }
            MultiPolygon(_, XY) => ContainsMultiPoint::contains(self.as_multi_polygon(), rhs),
            Mixed(_, XY) => ContainsMultiPoint::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsMultiPoint::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsMultiLineString<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: MultiLineStringTrait<T = f64>> ContainsMultiLineString<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_multi_line_string();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_multi_line_string {
    ($array:ty) => {
        impl<G: MultiLineStringTrait<T = f64>> ContainsMultiLineString<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = rhs.to_multi_line_string();
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_multi_line_string!(LineStringArray);
impl_contains_multi_line_string!(PolygonArray);
impl_contains_multi_line_string!(MultiPointArray);
impl_contains_multi_line_string!(MultiLineStringArray);
impl_contains_multi_line_string!(MultiPolygonArray);
impl_contains_multi_line_string!(MixedGeometryArray);
impl_contains_multi_line_string!(GeometryCollectionArray);

impl<G: MultiLineStringTrait<T = f64>> ContainsMultiLineString<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsMultiLineString::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsMultiLineString::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsMultiLineString::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => ContainsMultiLineString::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => {
                ContainsMultiLineString::contains(self.as_multi_line_string(), rhs)
            }
            MultiPolygon(_, XY) => ContainsMultiLineString::contains(self.as_multi_polygon(), rhs),
            Mixed(_, XY) => ContainsMultiLineString::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsMultiLineString::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsMultiPolygon<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: MultiPolygonTrait<T = f64>> ContainsMultiPolygon<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_multi_polygon();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_multi_polygon {
    ($array:ty) => {
        impl<G: MultiPolygonTrait<T = f64>> ContainsMultiPolygon<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = rhs.to_multi_polygon();
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_multi_polygon!(LineStringArray);
impl_contains_multi_polygon!(PolygonArray);
impl_contains_multi_polygon!(MultiPointArray);
impl_contains_multi_polygon!(MultiLineStringArray);
impl_contains_multi_polygon!(MultiPolygonArray);
impl_contains_multi_polygon!(MixedGeometryArray);
impl_contains_multi_polygon!(GeometryCollectionArray);

impl<G: MultiPolygonTrait<T = f64>> ContainsMultiPolygon<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsMultiPolygon::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsMultiPolygon::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsMultiPolygon::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => ContainsMultiPolygon::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => {
                ContainsMultiPolygon::contains(self.as_multi_line_string(), rhs)
            }
            MultiPolygon(_, XY) => ContainsMultiPolygon::contains(self.as_multi_polygon(), rhs),
            Mixed(_, XY) => ContainsMultiPolygon::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsMultiPolygon::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsGeometry<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_geometry();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_geometry {
    ($array:ty) => {
        impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = rhs.to_geometry();
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_geometry!(LineStringArray);
impl_contains_geometry!(PolygonArray);
// impl_contains_geometry!(MultiPointArray); // Not implemented in geo
impl_contains_geometry!(MultiLineStringArray);
// impl_contains_geometry!(MultiPolygonArray); // Not implemented in geo
impl_contains_geometry!(MixedGeometryArray);
impl_contains_geometry!(GeometryCollectionArray);

impl<G: GeometryTrait<T = f64>> ContainsGeometry<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsGeometry::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsGeometry::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsGeometry::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => todo!(), // ContainsGeometry::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => ContainsGeometry::contains(self.as_multi_line_string(), rhs),
            MultiPolygon(_, XY) => todo!(), // ContainsGeometry::contains(self.as_multi_polygon(), rhs),
            Mixed(_, XY) => ContainsGeometry::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsGeometry::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}

pub trait ContainsGeometryCollection<Rhs> {
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}

impl<G: GeometryCollectionTrait<T = f64>> ContainsGeometryCollection<G> for PointArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        let rhs = rhs.to_geometry_collection();
        self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
            .unwrap()
    }
}

macro_rules! impl_contains_geometry_collection {
    ($array:ty) => {
        impl<G: GeometryCollectionTrait<T = f64>> ContainsGeometryCollection<G> for $array {
            fn contains(&self, rhs: &G) -> BooleanArray {
                let rhs = rhs.to_geometry_collection();
                self.try_unary_boolean::<_, GeoArrowError>(|geom| Ok(geom.to_geo().contains(&rhs)))
                    .unwrap()
            }
        }
    };
}

impl_contains_geometry_collection!(LineStringArray);
impl_contains_geometry_collection!(PolygonArray);
impl_contains_geometry_collection!(MultiPointArray);
impl_contains_geometry_collection!(MultiLineStringArray);
impl_contains_geometry_collection!(MultiPolygonArray);
impl_contains_geometry_collection!(MixedGeometryArray);
impl_contains_geometry_collection!(GeometryCollectionArray);

impl<G: GeometryCollectionTrait<T = f64>> ContainsGeometryCollection<G> for &dyn NativeArray {
    fn contains(&self, rhs: &G) -> BooleanArray {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => ContainsGeometryCollection::contains(self.as_point(), rhs),
            LineString(_, XY) => ContainsGeometryCollection::contains(self.as_line_string(), rhs),
            Polygon(_, XY) => ContainsGeometryCollection::contains(self.as_polygon(), rhs),
            MultiPoint(_, XY) => ContainsGeometryCollection::contains(self.as_multi_point(), rhs),
            MultiLineString(_, XY) => {
                ContainsGeometryCollection::contains(self.as_multi_line_string(), rhs)
            }
            MultiPolygon(_, XY) => {
                ContainsGeometryCollection::contains(self.as_multi_polygon(), rhs)
            }
            Mixed(_, XY) => ContainsGeometryCollection::contains(self.as_mixed(), rhs),
            GeometryCollection(_, XY) => {
                ContainsGeometryCollection::contains(self.as_geometry_collection(), rhs)
            }
            _ => panic!("incorrect type"), // _ => return Err(GeoArrowError::IncorrectType("".into())),
        }
    }
}
