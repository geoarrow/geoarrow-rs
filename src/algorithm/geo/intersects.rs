use crate::chunked_array::ChunkedArray;
use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use crate::indexed::array::*;
use crate::indexed::chunked::*;
use crate::io::geo::{
    geometry_collection_to_geo, geometry_to_geo, line_string_to_geo, multi_line_string_to_geo,
    multi_point_to_geo, multi_polygon_to_geo, point_to_geo, polygon_to_geo,
};
use crate::trait_::GeometryScalarTrait;
use arrow_array::{BooleanArray, OffsetSizeTrait};
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
        impl<'a, O: OffsetSizeTrait> Intersects<$second> for $first {
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
iter_geo_impl!(IndexedPointArray, IndexedLineStringArray<O>);
iter_geo_impl!(IndexedPointArray, IndexedPolygonArray<O>);
iter_geo_impl!(IndexedPointArray, IndexedMultiPointArray<O>);
iter_geo_impl!(IndexedPointArray, IndexedMultiLineStringArray<O>);
iter_geo_impl!(IndexedPointArray, IndexedMultiPolygonArray<O>);
iter_geo_impl!(IndexedPointArray, IndexedMixedGeometryArray<O>);
iter_geo_impl!(IndexedPointArray, IndexedGeometryCollectionArray<O>);

// Implementations on LineStringArray
iter_geo_impl!(IndexedLineStringArray<O>, IndexedPointArray);
iter_geo_impl!(IndexedLineStringArray<O>, IndexedLineStringArray<O>);
iter_geo_impl!(IndexedLineStringArray<O>, IndexedPolygonArray<O>);
iter_geo_impl!(IndexedLineStringArray<O>, IndexedMultiPointArray<O>);
iter_geo_impl!(IndexedLineStringArray<O>, IndexedMultiLineStringArray<O>);
iter_geo_impl!(IndexedLineStringArray<O>, IndexedMultiPolygonArray<O>);
iter_geo_impl!(IndexedLineStringArray<O>, IndexedMixedGeometryArray<O>);
iter_geo_impl!(IndexedLineStringArray<O>, IndexedGeometryCollectionArray<O>);

// Implementations on PolygonArray
iter_geo_impl!(IndexedPolygonArray<O>, IndexedPointArray);
iter_geo_impl!(IndexedPolygonArray<O>, IndexedLineStringArray<O>);
iter_geo_impl!(IndexedPolygonArray<O>, IndexedPolygonArray<O>);
iter_geo_impl!(IndexedPolygonArray<O>, IndexedMultiPointArray<O>);
iter_geo_impl!(IndexedPolygonArray<O>, IndexedMultiLineStringArray<O>);
iter_geo_impl!(IndexedPolygonArray<O>, IndexedMultiPolygonArray<O>);
iter_geo_impl!(IndexedPolygonArray<O>, IndexedMixedGeometryArray<O>);
iter_geo_impl!(IndexedPolygonArray<O>, IndexedGeometryCollectionArray<O>);

// Implementations on MultiPointArray
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedPointArray);
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedLineStringArray<O>);
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedPolygonArray<O>);
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedMultiPointArray<O>);
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedMultiLineStringArray<O>);
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedMultiPolygonArray<O>);
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedMixedGeometryArray<O>);
iter_geo_impl!(IndexedMultiPointArray<O>, IndexedGeometryCollectionArray<O>);

// Implementations on MultiLineStringArray
iter_geo_impl!(IndexedMultiLineStringArray<O>, IndexedPointArray);
iter_geo_impl!(IndexedMultiLineStringArray<O>, IndexedLineStringArray<O>);
iter_geo_impl!(IndexedMultiLineStringArray<O>, IndexedPolygonArray<O>);
iter_geo_impl!(IndexedMultiLineStringArray<O>, IndexedMultiPointArray<O>);
iter_geo_impl!(
    IndexedMultiLineStringArray<O>,
    IndexedMultiLineStringArray<O>
);
iter_geo_impl!(IndexedMultiLineStringArray<O>, IndexedMultiPolygonArray<O>);
iter_geo_impl!(IndexedMultiLineStringArray<O>, IndexedMixedGeometryArray<O>);
iter_geo_impl!(
    IndexedMultiLineStringArray<O>,
    IndexedGeometryCollectionArray<O>
);

// Implementations on MultiPolygonArray
iter_geo_impl!(IndexedMultiPolygonArray<O>, IndexedPointArray);
iter_geo_impl!(IndexedMultiPolygonArray<O>, IndexedLineStringArray<O>);
iter_geo_impl!(IndexedMultiPolygonArray<O>, IndexedPolygonArray<O>);
iter_geo_impl!(IndexedMultiPolygonArray<O>, IndexedMultiPointArray<O>);
iter_geo_impl!(IndexedMultiPolygonArray<O>, IndexedMultiLineStringArray<O>);
iter_geo_impl!(IndexedMultiPolygonArray<O>, IndexedMultiPolygonArray<O>);
iter_geo_impl!(IndexedMultiPolygonArray<O>, IndexedMixedGeometryArray<O>);
iter_geo_impl!(
    IndexedMultiPolygonArray<O>,
    IndexedGeometryCollectionArray<O>
);

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
        let rhs = point_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect(), |geom| geom.to_geo().intersects(&rhs))
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: PointTrait<T = f64>> IntersectsPoint<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = point_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect(), |geom| geom.to_geo().intersects(&rhs))
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: PointTrait<T = f64>> IntersectsPoint<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = point_to_geo(rhs);
        self.map(|chunk| IntersectsPoint::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: PointTrait<T = f64>> IntersectsPoint<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = point_to_geo(rhs);
                self.map(|chunk| IntersectsPoint::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);

pub trait IntersectsLineString<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = line_string_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> IntersectsLineString<G>
            for $struct_name
        {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = line_string_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: LineStringTrait<T = f64>> IntersectsLineString<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = line_string_to_geo(rhs);
        self.map(|chunk| IntersectsLineString::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> IntersectsLineString<G>
            for $struct_name
        {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = line_string_to_geo(rhs);
                self.map(|chunk| IntersectsLineString::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);

pub trait IntersectsPolygon<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = polygon_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> IntersectsPolygon<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = polygon_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: PolygonTrait<T = f64>> IntersectsPolygon<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = polygon_to_geo(rhs);
        self.map(|chunk| IntersectsPolygon::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> IntersectsPolygon<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = polygon_to_geo(rhs);
                self.map(|chunk| IntersectsPolygon::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);

pub trait IntersectsMultiPoint<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_point_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G>
            for $struct_name
        {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_point_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_point_to_geo(rhs);
        self.map(|chunk| IntersectsMultiPoint::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> IntersectsMultiPoint<G>
            for $struct_name
        {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_point_to_geo(rhs);
                self.map(|chunk| IntersectsMultiPoint::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);

pub trait IntersectsMultiLineString<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_line_string_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G>
            for $struct_name
        {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_line_string_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_line_string_to_geo(rhs);
        self.map(|chunk| IntersectsMultiLineString::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>> IntersectsMultiLineString<G>
            for $struct_name
        {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_line_string_to_geo(rhs);
                self.map(|chunk| IntersectsMultiLineString::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);

pub trait IntersectsMultiPolygon<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_polygon_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G>
            for $struct_name
        {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_polygon_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = multi_polygon_to_geo(rhs);
        self.map(|chunk| IntersectsMultiPolygon::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> IntersectsMultiPolygon<G>
            for $struct_name
        {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = multi_polygon_to_geo(rhs);
                self.map(|chunk| IntersectsMultiPolygon::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);

pub trait IntersectsGeometry<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> IntersectsGeometry<G> for $struct_name {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: GeometryTrait<T = f64>> IntersectsGeometry<G> for IndexedChunkedPointArray {
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_to_geo(rhs);
        self.map(|chunk| IntersectsGeometry::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: GeometryTrait<T = f64>> IntersectsGeometry<G> for $struct_name {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_to_geo(rhs);
                self.map(|chunk| IntersectsGeometry::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);

pub trait IntersectsGeometryCollection<Rhs> {
    type Output;

    fn intersects(&self, rhs: &Rhs) -> Self::Output;
}

impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G> for IndexedPointArray {
    type Output = BooleanArray;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_collection_to_geo(rhs);
        self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
            geom.to_geo().intersects(&rhs)
        })
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>>
            IntersectsGeometryCollection<G> for $struct_name
        {
            type Output = BooleanArray;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_collection_to_geo(rhs);
                self.unary_boolean(&rhs.bounding_rect().unwrap(), |geom| {
                    geom.to_geo().intersects(&rhs)
                })
            }
        }
    };
}

impl_intersects!(IndexedLineStringArray<O>);
impl_intersects!(IndexedPolygonArray<O>);
impl_intersects!(IndexedMultiPointArray<O>);
impl_intersects!(IndexedMultiLineStringArray<O>);
impl_intersects!(IndexedMultiPolygonArray<O>);
impl_intersects!(IndexedMixedGeometryArray<O>);
impl_intersects!(IndexedGeometryCollectionArray<O>);

impl<G: GeometryCollectionTrait<T = f64>> IntersectsGeometryCollection<G>
    for IndexedChunkedPointArray
{
    type Output = ChunkedArray<BooleanArray>;

    fn intersects(&self, rhs: &G) -> Self::Output {
        let rhs = geometry_collection_to_geo(rhs);
        self.map(|chunk| IntersectsGeometryCollection::intersects(chunk, &rhs))
            .try_into()
            .unwrap()
    }
}

macro_rules! impl_intersects {
    ($struct_name:ty) => {
        impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>>
            IntersectsGeometryCollection<G> for $struct_name
        {
            type Output = ChunkedArray<BooleanArray>;

            fn intersects(&self, rhs: &G) -> Self::Output {
                let rhs = geometry_collection_to_geo(rhs);
                self.map(|chunk| IntersectsGeometryCollection::intersects(chunk, &rhs))
                    .try_into()
                    .unwrap()
            }
        }
    };
}

impl_intersects!(IndexedChunkedLineStringArray<O>);
impl_intersects!(IndexedChunkedPolygonArray<O>);
impl_intersects!(IndexedChunkedMultiPointArray<O>);
impl_intersects!(IndexedChunkedMultiLineStringArray<O>);
impl_intersects!(IndexedChunkedMultiPolygonArray<O>);
impl_intersects!(IndexedChunkedMixedGeometryArray<O>);
impl_intersects!(IndexedChunkedGeometryCollectionArray<O>);
