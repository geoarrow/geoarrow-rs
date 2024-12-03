use crate::chunked_array::ChunkedArray;
use crate::indexed::array::*;
use crate::indexed::chunked::*;
use crate::trait_::NativeScalar;
use arrow_array::BooleanArray;
use geo::{BoundingRect, Intersects as _Intersects};
use geo_traits::to_geo::*;
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};

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
impl Intersects for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &Self) -> Self::Output {
        self.try_binary_boolean(rhs, |left, right| {
            Ok(left.to_geo().intersects(&right.to_geo()))
        })
        .unwrap()
    }
}

// Implementation that iterates over geo objects
macro_rules! iter_geo_impl {
    ($first:ty, $second:ty) => {
        impl<'a> Intersects<$second> for $first {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &$second) -> Self::Output {
                self.try_binary_boolean(rhs, |left, right| {
                    Ok(left.to_geo().intersects(&right.to_geo()))
                })
                .unwrap()
            }
        }
    };
}

// Implementations on PointArray
iter_geo_impl!(IndexedPointArray, IndexedLineStringArray);
iter_geo_impl!(IndexedPointArray, IndexedPolygonArray);
iter_geo_impl!(IndexedPointArray, IndexedMultiPointArray);
iter_geo_impl!(IndexedPointArray, IndexedMultiLineStringArray);
iter_geo_impl!(IndexedPointArray, IndexedMultiPolygonArray);
iter_geo_impl!(IndexedPointArray, IndexedMixedGeometryArray);
iter_geo_impl!(IndexedPointArray, IndexedGeometryCollectionArray);

// Implementations on LineStringArray
iter_geo_impl!(IndexedLineStringArray, IndexedPointArray);
iter_geo_impl!(IndexedLineStringArray, IndexedLineStringArray);
iter_geo_impl!(IndexedLineStringArray, IndexedPolygonArray);
iter_geo_impl!(IndexedLineStringArray, IndexedMultiPointArray);
iter_geo_impl!(IndexedLineStringArray, IndexedMultiLineStringArray);
iter_geo_impl!(IndexedLineStringArray, IndexedMultiPolygonArray);
iter_geo_impl!(IndexedLineStringArray, IndexedMixedGeometryArray);
iter_geo_impl!(IndexedLineStringArray, IndexedGeometryCollectionArray);

// Implementations on PolygonArray
iter_geo_impl!(IndexedPolygonArray, IndexedPointArray);
iter_geo_impl!(IndexedPolygonArray, IndexedLineStringArray);
iter_geo_impl!(IndexedPolygonArray, IndexedPolygonArray);
iter_geo_impl!(IndexedPolygonArray, IndexedMultiPointArray);
iter_geo_impl!(IndexedPolygonArray, IndexedMultiLineStringArray);
iter_geo_impl!(IndexedPolygonArray, IndexedMultiPolygonArray);
iter_geo_impl!(IndexedPolygonArray, IndexedMixedGeometryArray);
iter_geo_impl!(IndexedPolygonArray, IndexedGeometryCollectionArray);

// Implementations on MultiPointArray
iter_geo_impl!(IndexedMultiPointArray, IndexedPointArray);
iter_geo_impl!(IndexedMultiPointArray, IndexedLineStringArray);
iter_geo_impl!(IndexedMultiPointArray, IndexedPolygonArray);
iter_geo_impl!(IndexedMultiPointArray, IndexedMultiPointArray);
iter_geo_impl!(IndexedMultiPointArray, IndexedMultiLineStringArray);
iter_geo_impl!(IndexedMultiPointArray, IndexedMultiPolygonArray);
iter_geo_impl!(IndexedMultiPointArray, IndexedMixedGeometryArray);
iter_geo_impl!(IndexedMultiPointArray, IndexedGeometryCollectionArray);

// Implementations on MultiLineStringArray
iter_geo_impl!(IndexedMultiLineStringArray, IndexedPointArray);
iter_geo_impl!(IndexedMultiLineStringArray, IndexedLineStringArray);
iter_geo_impl!(IndexedMultiLineStringArray, IndexedPolygonArray);
iter_geo_impl!(IndexedMultiLineStringArray, IndexedMultiPointArray);
iter_geo_impl!(IndexedMultiLineStringArray, IndexedMultiLineStringArray);
iter_geo_impl!(IndexedMultiLineStringArray, IndexedMultiPolygonArray);
iter_geo_impl!(IndexedMultiLineStringArray, IndexedMixedGeometryArray);
iter_geo_impl!(IndexedMultiLineStringArray, IndexedGeometryCollectionArray);

// Implementations on MultiPolygonArray
iter_geo_impl!(IndexedMultiPolygonArray, IndexedPointArray);
iter_geo_impl!(IndexedMultiPolygonArray, IndexedLineStringArray);
iter_geo_impl!(IndexedMultiPolygonArray, IndexedPolygonArray);
iter_geo_impl!(IndexedMultiPolygonArray, IndexedMultiPointArray);
iter_geo_impl!(IndexedMultiPolygonArray, IndexedMultiLineStringArray);
iter_geo_impl!(IndexedMultiPolygonArray, IndexedMultiPolygonArray);
iter_geo_impl!(IndexedMultiPolygonArray, IndexedMixedGeometryArray);
iter_geo_impl!(IndexedMultiPolygonArray, IndexedGeometryCollectionArray);

// ┌─────────────────────────────────┐
// │ Implementations for RHS scalars │
// └─────────────────────────────────┘

pub trait IntersectsPoint<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: PointTrait<T = f64>> IntersectsPoint<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_point();
        self.unary_boolean(&rhs.bounding_rect(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PointTrait<T = f64>> IntersectsPoint<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_point();
                self.unary_boolean(&rhs.bounding_rect(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: PointTrait<T = f64>> IntersectsPoint<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_point();
        self.map(|chunk| IntersectsPoint::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PointTrait<T = f64>> IntersectsPoint<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_point();
                self.map(|chunk| IntersectsPoint::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);

pub trait IntersectsLineString<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_line_string();
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_line_string();
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_line_string();
        self.map(|chunk| IntersectsLineString::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_line_string();
                self.map(|chunk| IntersectsLineString::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);

pub trait IntersectsPolygon<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_polygon();
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_polygon();
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_polygon();
        self.map(|chunk| IntersectsPolygon::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_polygon();
                self.map(|chunk| IntersectsPolygon::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);

pub trait IntersectsMultiPoint<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_multi_point();
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_multi_point();
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_multi_point();
        self.map(|chunk| IntersectsMultiPoint::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_multi_point();
                self.map(|chunk| IntersectsMultiPoint::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);

pub trait IntersectsMultiLineString<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_multi_line_string();
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_multi_line_string();
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_multi_line_string();
        self.map(|chunk| IntersectsMultiLineString::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_multi_line_string();
                self.map(|chunk| IntersectsMultiLineString::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);

pub trait IntersectsMultiPolygon<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_multi_polygon();
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_multi_polygon();
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_multi_polygon();
        self.map(|chunk| IntersectsMultiPolygon::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_multi_polygon();
                self.map(|chunk| IntersectsMultiPolygon::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);

pub trait IntersectsGeometry<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_geometry();
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_geometry();
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_geometry();
        self.map(|chunk| IntersectsGeometry::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_geometry();
                self.map(|chunk| IntersectsGeometry::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);

pub trait IntersectsGeometryCollection<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_geometry_collection();
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_geometry_collection();
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray);
impl_intersects!(IndexedPolygonArray);
impl_intersects!(IndexedMultiPointArray);
impl_intersects!(IndexedMultiLineStringArray);
impl_intersects!(IndexedMultiPolygonArray);
impl_intersects!(IndexedMixedGeometryArray);
impl_intersects!(IndexedGeometryCollectionArray);

impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G>
    for IndexedChunkedPointArray
{
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = rhs.to_geometry_collection();
        self.map(|chunk| IntersectsGeometryCollection::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = rhs.to_geometry_collection();
                self.map(|chunk| IntersectsGeometryCollection::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray);
impl_intersects!(IndexedChunkedPolygonArray);
impl_intersects!(IndexedChunkedMultiPointArray);
impl_intersects!(IndexedChunkedMultiLineStringArray);
impl_intersects!(IndexedChunkedMultiPolygonArray);
impl_intersects!(IndexedChunkedMixedGeometryArray);
impl_intersects!(IndexedChunkedGeometryCollectionArray);
