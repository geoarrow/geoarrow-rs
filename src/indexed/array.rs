use std::sync::Arc;

use crate::algorithm::geo_index::RTree;
use crate::array::*;
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{CoordTrait, RectTrait};
use crate::scalar::Geometry;
use crate::trait_::{ArrayAccessor, NativeGeometryAccessor};
use crate::NativeArray;
use arrow_array::builder::BooleanBuilder;
use arrow_array::BooleanArray;
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use geo_index::rtree::{OwnedRTree, RTreeIndex};

// TODO: also store Option<ValidOffsets>
// The problem is that the RTree is only able to store valid, non-empty geometries. But the
// GeometryArray is able to store missing and empty geometries. So we need a mapping from _valid_
// geometry in the tree back to the actual row index it came from.
#[allow(dead_code)]
pub struct IndexedGeometryArray<const D: usize> {
    pub(crate) array: Arc<dyn NativeGeometryAccessor<D>>,
    pub(crate) index: OwnedRTree<f64>,
}

impl<const D: usize> IndexedGeometryArray<D> {
    #[allow(dead_code)]
    pub fn new<A: NativeGeometryAccessor<D>>(array: Arc<dyn NativeGeometryAccessor<D>>) -> Self {
        assert_eq!(array.null_count(), 0);
        let index = array.as_native_array().create_rtree();
        Self { array, index }
    }

    #[allow(dead_code)]
    pub fn data_type(&self) -> NativeType {
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

    pub fn intersection_candidates_with_other<'a, const D2: usize>(
        &'a self,
        other: &'a IndexedGeometryArray<D2>,
    ) -> impl Iterator<Item = (usize, usize)> + 'a {
        self.index
            .intersection_candidates_with_other_tree(&other.index)
    }
}

impl<'a, const D: usize> IndexedGeometryArray<D> {
    /// Intended for e.g. intersects against a scalar with a single bounding box
    pub fn unary_boolean<F>(&'a self, rhs_rect: &impl RectTrait<T = f64>, op: F) -> BooleanArray
    where
        F: Fn(&Geometry<D>) -> bool,
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
            buffer.set_bit(
                candidate_idx,
                op(&self.array.value_as_geometry(candidate_idx)),
            );
        }

        BooleanArray::new(buffer.finish(), nulls)
    }

    /// A helper function for boolean operations where it only applies `op` to pairs whose bounding
    /// boxes intersect.
    ///
    /// Note that this only compares pairs at the same row index.
    pub fn try_binary_boolean<F, const D2: usize>(
        &'a self,
        other: &'a IndexedGeometryArray<D2>,
        op: F,
    ) -> Result<BooleanArray>
    where
        F: Fn(&Geometry<D>, &Geometry<D2>) -> Result<bool>,
    {
        if self.len() != other.len() {
            return Err(GeoArrowError::General(
                "Cannot perform binary operation on arrays of different length".to_string(),
            ));
        }

        if self.is_empty() {
            return Ok(BooleanBuilder::new().finish());
        }

        let nulls = NullBuffer::union(self.array.nulls(), other.array.nulls());
        let mut builder_buffer = BooleanBufferBuilder::new(self.len());
        builder_buffer.append_n(self.len(), false);

        for (left_candidate_idx, right_candidate_idx) in
            self.intersection_candidates_with_other(other)
        {
            if left_candidate_idx != right_candidate_idx {
                continue;
            }

            let left = self.array.value_as_geometry(left_candidate_idx);
            let right = other.array.value_as_geometry(right_candidate_idx);

            builder_buffer.set_bit(left_candidate_idx, op(&left, &right)?);
        }

        Ok(BooleanArray::new(builder_buffer.finish(), nulls))
    }
}

impl<const D: usize> RTreeIndex<f64> for IndexedGeometryArray<D> {
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
