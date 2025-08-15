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
        impl<'a> RTree<'a> for $type {
            type RTreeObject = CachedEnvelope<$scalar_type>;

            fn rstar_tree(&'a self) -> rstar::RTree<Self::RTreeObject> {
                rstar::RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
            }
        }
    };
}

iter_cached_impl!(LineStringArray, crate::scalar::LineString<'a>);
iter_cached_impl!(PolygonArray, crate::scalar::Polygon<'a>);
iter_cached_impl!(MultiPointArray, crate::scalar::MultiPoint<'a>);
iter_cached_impl!(MultiLineStringArray, crate::scalar::MultiLineString<'a>);
iter_cached_impl!(MultiPolygonArray, crate::scalar::MultiPolygon<'a>);
iter_cached_impl!(MixedGeometryArray, crate::scalar::Geometry<'a>);
iter_cached_impl!(
    GeometryCollectionArray,
    crate::scalar::GeometryCollection<'a>
);
