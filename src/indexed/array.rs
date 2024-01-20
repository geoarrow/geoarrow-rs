use std::sync::Arc;

use crate::algorithm::geo_index::RTree;
use crate::array::*;
use crate::GeometryArrayTrait;
use geo_index::rtree::OwnedRTree;

#[allow(dead_code)]
pub struct IndexedGeometryArray<G: GeometryArrayTrait> {
    pub(crate) array: G,
    pub(crate) index: OwnedRTree<f64>,
}

impl<G: GeometryArrayTrait> IndexedGeometryArray<G> {
    pub fn new(array: G) -> Self {
        assert_eq!(array.null_count(), 0);
        let index = array.as_ref().create_rtree();
        Self { array, index }
    }
}

pub type IndexedPointArray = IndexedGeometryArray<PointArray>;
pub type IndexedLineStringArray<O> = IndexedGeometryArray<LineStringArray<O>>;
pub type IndexedPolygonArray<O> = IndexedGeometryArray<PolygonArray<O>>;
pub type IndexedMultiPointArray<O> = IndexedGeometryArray<MultiPointArray<O>>;
pub type IndexedMultiLineStringArray<O> = IndexedGeometryArray<MultiLineStringArray<O>>;
pub type IndexedMultiPolygonArray<O> = IndexedGeometryArray<MultiPolygonArray<O>>;
pub type IndexedMixedGeometryArray<O> = IndexedGeometryArray<MixedGeometryArray<O>>;
pub type IndexedGeometryCollectionArray<O> = IndexedGeometryArray<GeometryCollectionArray<O>>;
pub type IndexedWKBArray<O> = IndexedGeometryArray<WKBArray<O>>;
pub type IndexedRectArray = IndexedGeometryArray<RectArray>;
pub type IndexedUnknownGeometryArray = IndexedGeometryArray<Arc<dyn GeometryArrayTrait>>;
