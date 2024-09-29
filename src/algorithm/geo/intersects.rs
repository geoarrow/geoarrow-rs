use crate::chunked_array::ChunkedArray;
use crate::geo_traits::{GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait};
use crate::indexed::array::*;
use crate::indexed::chunked::*;
use crate::io::geo::{geometry_collection_to_geo, geometry_to_geo, line_string_to_geo, multi_line_string_to_geo, multi_point_to_geo, multi_polygon_to_geo, point_to_geo, polygon_to_geo};
use crate::trait_::NativeScalar;
use arrow_array::BooleanArray;
use geo::{BoundingRect, Intersects as _Intersects};

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
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

// Note: this implementation is outside the macro because it is not generic over O
impl Intersects for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &Self) -> Self::Output {
        self.try_binary_boolean(rhs, |left, right| Ok(left.to_geo().intersects(&right.to_geo()))).unwrap()
    }
}

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a> Intersects<$second> for $first {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &$second) -> Self::Output {
                self.try_binary_boolean(rhs, |left, right| Ok(left.to_geo().intersects(&right.to_geo()))).unwrap()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(IndexedPointArray<2>, IndexedLineStringArray<2>);
iter_geo_impl!(IndexedPointArray<2>, IndexedPolygonArray<2>);
iter_geo_impl!(IndexedPointArray<2>, IndexedMultiPointArray<2>);
iter_geo_impl!(IndexedPointArray<2>, IndexedMultiLineStringArray<2>);
iter_geo_impl!(IndexedPointArray<2>, IndexedMultiPolygonArray<2>);
iter_geo_impl!(IndexedPointArray<2>, IndexedMixedGeometryArray<2>);
iter_geo_impl!(IndexedPointArray<2>, IndexedGeometryCollectionArray<2>);

// Implementations on LineStringArray
iter_geo_impl!(IndexedLineStringArray<2>, IndexedPointArray<2>);
iter_geo_impl!(IndexedLineStringArray<2>, IndexedLineStringArray<2>);
iter_geo_impl!(IndexedLineStringArray<2>, IndexedPolygonArray<2>);
iter_geo_impl!(IndexedLineStringArray<2>, IndexedMultiPointArray<2>);
iter_geo_impl!(IndexedLineStringArray<2>, IndexedMultiLineStringArray<2>);
iter_geo_impl!(IndexedLineStringArray<2>, IndexedMultiPolygonArray<2>);
iter_geo_impl!(IndexedLineStringArray<2>, IndexedMixedGeometryArray<2>);
iter_geo_impl!(IndexedLineStringArray<2>, IndexedGeometryCollectionArray<2>);

// Implementations on PolygonArray
iter_geo_impl!(IndexedPolygonArray<2>, IndexedPointArray<2>);
iter_geo_impl!(IndexedPolygonArray<2>, IndexedLineStringArray<2>);
iter_geo_impl!(IndexedPolygonArray<2>, IndexedPolygonArray<2>);
iter_geo_impl!(IndexedPolygonArray<2>, IndexedMultiPointArray<2>);
iter_geo_impl!(IndexedPolygonArray<2>, IndexedMultiLineStringArray<2>);
iter_geo_impl!(IndexedPolygonArray<2>, IndexedMultiPolygonArray<2>);
iter_geo_impl!(IndexedPolygonArray<2>, IndexedMixedGeometryArray<2>);
iter_geo_impl!(IndexedPolygonArray<2>, IndexedGeometryCollectionArray<2>);

// Implementations on MultiPointArray
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedPointArray<2>);
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedLineStringArray<2>);
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedPolygonArray<2>);
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedMultiPointArray<2>);
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedMultiLineStringArray<2>);
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedMultiPolygonArray<2>);
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedMixedGeometryArray<2>);
iter_geo_impl!(IndexedMultiPointArray<2>, IndexedGeometryCollectionArray<2>);

// Implementations on MultiLineStringArray
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedPointArray<2>);
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedLineStringArray<2>);
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedPolygonArray<2>);
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedMultiPointArray<2>);
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedMultiLineStringArray<2>);
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedMultiPolygonArray<2>);
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedMixedGeometryArray<2>);
iter_geo_impl!(IndexedMultiLineStringArray<2>, IndexedGeometryCollectionArray<2>);

// Implementations on MultiPolygonArray
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedPointArray<2>);
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedLineStringArray<2>);
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedPolygonArray<2>);
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedMultiPointArray<2>);
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedMultiLineStringArray<2>);
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedMultiPolygonArray<2>);
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedMixedGeometryArray<2>);
iter_geo_impl!(IndexedMultiPolygonArray<2>, IndexedGeometryCollectionArray<2>);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

pub trait IntersectsPoint<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: PointTrait<T = f64>> IntersectsPoint<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = point_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PointTrait<T = f64>> IntersectsPoint<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = point_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: PointTrait<T = f64>> IntersectsPoint<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = point_to_geo(rhs);
        self.map(|chunk| IntersectsPoint::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PointTrait<T = f64>> IntersectsPoint<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = point_to_geo(rhs);
                self.map(|chunk| IntersectsPoint::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);

pub trait IntersectsLineString<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = line_string_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = line_string_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = line_string_to_geo(rhs);
        self.map(|chunk| IntersectsLineString::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = line_string_to_geo(rhs);
                self.map(|chunk| IntersectsLineString::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);

pub trait IntersectsPolygon<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = polygon_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = polygon_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = polygon_to_geo(rhs);
        self.map(|chunk| IntersectsPolygon::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = polygon_to_geo(rhs);
                self.map(|chunk| IntersectsPolygon::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);

pub trait IntersectsMultiPoint<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_point_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_point_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_point_to_geo(rhs);
        self.map(|chunk| IntersectsMultiPoint::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_point_to_geo(rhs);
                self.map(|chunk| IntersectsMultiPoint::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);

pub trait IntersectsMultiLineString<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_line_string_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_line_string_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_line_string_to_geo(rhs);
        self.map(|chunk| IntersectsMultiLineString::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_line_string_to_geo(rhs);
                self.map(|chunk| IntersectsMultiLineString::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);

pub trait IntersectsMultiPolygon<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_polygon_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_polygon_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_polygon_to_geo(rhs);
        self.map(|chunk| IntersectsMultiPolygon::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_polygon_to_geo(rhs);
                self.map(|chunk| IntersectsMultiPolygon::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);

pub trait IntersectsGeometry<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_to_geo(rhs);
        self.map(|chunk| IntersectsGeometry::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_to_geo(rhs);
                self.map(|chunk| IntersectsGeometry::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);

pub trait IntersectsGeometryCollection<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for IndexedPointArray<2> {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_collection_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_collection_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<2>);
impl_intersects!(IndexedPolygonArray<2>);
impl_intersects!(IndexedMultiPointArray<2>);
impl_intersects!(IndexedMultiLineStringArray<2>);
impl_intersects!(IndexedMultiPolygonArray<2>);
impl_intersects!(IndexedMixedGeometryArray<2>);
impl_intersects!(IndexedGeometryCollectionArray<2>);

impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for IndexedChunkedPointArray<2> {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_collection_to_geo(rhs);
        self.map(|chunk| IntersectsGeometryCollection::intersects(chunk, &rhs)).try_into().unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_collection_to_geo(rhs);
                self.map(|chunk| IntersectsGeometryCollection::intersects(chunk, &rhs)).try_into().unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<2>);
impl_intersects!(IndexedChunkedPolygonArray<2>);
impl_intersects!(IndexedChunkedMultiPointArray<2>);
impl_intersects!(IndexedChunkedMultiLineStringArray<2>);
impl_intersects!(IndexedChunkedMultiPolygonArray<2>);
impl_intersects!(IndexedChunkedMixedGeometryArray<2>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<2>);
