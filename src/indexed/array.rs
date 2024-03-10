use std::sync::Arc;

use crate::algorithm::geo_index::RTree;
use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, RectTrait};
use crate::trait_::GeometryArrayAccessor;
use crate::GeometryArrayTrait;
use arrow_array::builder::BooleanBuilder;
use arrow_array::BooleanArray;
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use geo_index::rtree::{OwnedRTree, RTreeIndex};

// TODO: also store Option<ValidOffsets>
// The problem is that the RTree is only able to store valid, non-empty geometries. But the
// GeometryArray is able to store missing and empty geometries. So we need a mapping from _valid_
// geometry in the tree back to the actual row index it came from.
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

    pub fn data_type(&self) -> &GeoDataType {
        self.array.data_type()
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn search(&self, min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Vec<usize> {
        self.index.search(min_x, min_y, max_x, max_y)
    }

    pub fn intersection_candidates_with_other<'a, G2: GeometryArrayTrait>(
        &'a self,
        other: &'a IndexedGeometryArray<G2>,
    ) -> impl Iterator<Item = (usize, usize)> + 'a {
        self.index
            .intersection_candidates_with_other_tree(&other.index)
    }
}

impl<'a, G: GeometryArrayTrait + GeometryArrayAccessor<'a>> IndexedGeometryArray<G> {
    /// Intended for e.g. intersects against a scalar with a single bounding box
    pub fn unary_boolean<F>(&'a self, rhs_rect: &impl RectTrait<T = f64>, op: F) -> BooleanArray
    where
        F: Fn(G::Item) -> bool,
    {
        let len = self.len();

        let nulls = self.array.nulls().cloned();
        let mut buffer = BooleanBufferBuilder::new(len);
        buffer.append_n(len, false);

        // TODO: ensure this is only on valid indexes
        for candidate_idx in self.search(
            rhs_rect.lower().x(),
            rhs_rect.lower().y(),
            rhs_rect.upper().x(),
            rhs_rect.upper().y(),
        ) {
            buffer.set_bit(candidate_idx, op(self.array.value(candidate_idx)));
        }

        BooleanArray::new(buffer.finish(), nulls)
    }

    pub fn try_binary_boolean<F, G2>(
        &'a self,
        other: &'a IndexedGeometryArray<G2>,
        op: F,
    ) -> Result<BooleanArray>
    where
        G2: GeometryArrayTrait + GeometryArrayAccessor<'a>,
        F: Fn(G::Item, G2::Item) -> Result<bool>,
    {
        if self.len() != other.len() {
            return Err(GeoArrowError::General(
                "Cannot perform binary operation on arrays of different length".to_string(),
            ));
        }

        if self.is_empty() {
            return Ok(BooleanBuilder::new().finish());
        }

        let nulls = NullBuffer::union(
            self.array.logical_nulls().as_ref(),
            other.array.logical_nulls().as_ref(),
        );
        let mut builder_buffer = BooleanBufferBuilder::new(self.len());
        builder_buffer.append_n(self.len(), false);

        for (left_candidate_idx, right_candidate_idx) in
            self.intersection_candidates_with_other(other)
        {
            if left_candidate_idx != right_candidate_idx {
                continue;
            }

            let left = self.array.value(left_candidate_idx);
            let right = other.array.value(right_candidate_idx);

            builder_buffer.set_bit(left_candidate_idx, op(left, right)?);
        }

        Ok(BooleanArray::new(builder_buffer.finish(), nulls))
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

impl<G: GeometryArrayTrait> RTreeIndex<f64> for IndexedGeometryArray<G> {
    fn boxes(&self) -> &[f64] {
        self.index.boxes()
    }

    fn indices(&self) -> std::borrow::Cow<'_, geo_index::indices::Indices> {
        self.index.indices()
    }

    fn num_items(&self) -> usize {
        self.index.num_items()
    }

    fn num_nodes(&self) -> usize {
        self.index.num_nodes()
    }

    fn node_size(&self) -> usize {
        self.index.node_size()
    }

    fn level_bounds(&self) -> &[usize] {
        self.index.level_bounds()
    }
}
