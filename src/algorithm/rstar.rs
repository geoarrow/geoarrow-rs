//! Bindings to the [`rstar`] crate for dynamic R-Trees.

use crate::array::*;
use crate::trait_::GeometryArrayAccessor;
use arrow_array::OffsetSizeTrait;
use rstar::primitives::CachedEnvelope;

/// Construct an R-Tree from a geometry array.
pub trait RTree<'a> {
    /// The object type to store in the RTree.
    type RTreeObject: rstar::RTreeObject;

    /// Build an [`RTree`] spatial index containing this array's geometries.
    fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject>;
}

impl<'a> RTree<'a> for PointArray {
    type RTreeObject = crate::scalar::Point<'a>;

    fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject> {
        // Note: for points we don't memoize with CachedEnvelope
        rstar::RTree::bulk_load(self.iter().flatten().collect())
    }
}

impl<'a> RTree<'a> for RectArray {
    type RTreeObject = crate::scalar::Rect<'a>;

    fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject> {
        // Note: for rects we don't memoize with CachedEnvelope
        rstar::RTree::bulk_load(self.iter().flatten().collect())
    }
}

macro_rules! iter_cached_impl {
    ($type:ty, $scalar_type:ty) => {
        impl<'a, O: OffsetSizeTrait> RTree<'a> for $type {
            type RTreeObject = CachedEnvelope<$scalar_type>;

            fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject> {
                rstar::RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
            }
        }
    };
}

iter_cached_impl!(LineStringArray<O>, crate::scalar::LineString<'a, O>);
iter_cached_impl!(PolygonArray<O>, crate::scalar::Polygon<'a, O>);
iter_cached_impl!(MultiPointArray<O>, crate::scalar::MultiPoint<'a, O>);
iter_cached_impl!(
    MultiLineStringArray<O>,
    crate::scalar::MultiLineString<'a, O>
);
iter_cached_impl!(MultiPolygonArray<O>, crate::scalar::MultiPolygon<'a, O>);
iter_cached_impl!(WKBArray<O>, crate::scalar::WKB<'a, O>);
iter_cached_impl!(MixedGeometryArray<O>, crate::scalar::Geometry<'a, O>);
iter_cached_impl!(
    GeometryCollectionArray<O>,
    crate::scalar::GeometryCollection<'a, O>
);
