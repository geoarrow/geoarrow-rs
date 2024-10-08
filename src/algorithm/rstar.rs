//! Bindings to the [`rstar`] crate for dynamic R-Trees.

use crate::array::*;
use crate::trait_::ArrayAccessor;
use rstar::primitives::CachedEnvelope;

/// Construct an R-Tree from a geometry array.
pub trait RTree<'a> {
    /// The object type to store in the RTree.
    type RTreeObject: rstar::RTreeObject;

    /// Build an [`RTree`] spatial index containing this array's geometries.
    fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject>;
}

impl<'a> RTree<'a> for PointArray<2> {
    type RTreeObject = crate::scalar::Point<'a, 2>;

    fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject> {
        // Note: for points we don't memoize with CachedEnvelope
        rstar::RTree::bulk_load(self.iter().flatten().collect())
    }
}

impl<'a> RTree<'a> for RectArray<2> {
    type RTreeObject = crate::scalar::Rect<'a, 2>;

    fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject> {
        // Note: for rects we don't memoize with CachedEnvelope
        rstar::RTree::bulk_load(self.iter().flatten().collect())
    }
}

macro_rules! iter_cached_impl {
    ($type:ty, $scalar_type:ty) => {
        impl<'a> RTree<'a> for $type {
            type RTreeObject = CachedEnvelope<$scalar_type>;

            fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject> {
                rstar::RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
            }
        }
    };
}

iter_cached_impl!(LineStringArray<2>, crate::scalar::LineString<'a, 2>);
iter_cached_impl!(PolygonArray<2>, crate::scalar::Polygon<'a, 2>);
iter_cached_impl!(MultiPointArray<2>, crate::scalar::MultiPoint<'a, 2>);
iter_cached_impl!(
    MultiLineStringArray<2>,
    crate::scalar::MultiLineString<'a, 2>
);
iter_cached_impl!(MultiPolygonArray<2>, crate::scalar::MultiPolygon<'a, 2>);
iter_cached_impl!(MixedGeometryArray<2>, crate::scalar::Geometry<'a, 2>);
iter_cached_impl!(
    GeometryCollectionArray<2>,
    crate::scalar::GeometryCollection<'a, 2>
);
